"""
api/webhook.py
==============
FastAPI app — recebe webhooks do Twilio WhatsApp, persiste a Message
e dispara o workflow Celery em fila.

Contrato com a Twilio:
  - Endpoint:  POST /webhook/twilio
  - Mime:      application/x-www-form-urlencoded (Twilio padrão)
  - Resposta:  200 OK + TwiML vazio (Twilio aceita; evita auto-resposta)
  - SLA:       a Twilio considera a entrega bem-sucedida se respondermos
               em até ~15s — por isso despachamos a task assíncrona ao Celery
               e retornamos imediatamente (latência típica < 100ms).

Segurança:
  - Toda requisição é validada via `twilio.request_validator.RequestValidator`
    com o `TWILIO_AUTH_TOKEN`. Falhas devolvem 403.
  - O bypass de validação só é permitido se `TWILIO_VALIDATE_SIGNATURE=false`
    estiver explicitamente setado (modo dev/local). Default = strict.

Idempotência:
  - A Twilio retenta webhooks que falham (5xx/timeout). A task downstream
    (`ExtractorAgent.process_single_message`) é idempotente — ela consulta
    `Deal.source_message_id` antes de criar. Já o INSERT de Message neste
    handler usa o `MessageSid` do Twilio como chave natural via lookup
    pré-INSERT, evitando duplicação no banco.

Roda como:
    uvicorn api.webhook:app --host 0.0.0.0 --port 8000

E expõe ao Twilio via tunnel (ngrok/Cloudflare) durante o desenvolvimento.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import PlainTextResponse

logger = logging.getLogger("samba.webhook")

# ----------------------------------------------------------------------------
# Config (env)
# ----------------------------------------------------------------------------

TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
VALIDATE_SIGNATURE = os.getenv("TWILIO_VALIDATE_SIGNATURE", "true").lower() != "false"

# A URL pública que o Twilio chama (pode diferir da URL interna por causa
# de proxies/tunnels). Se não setada, derivamos do request — mas isso só
# funciona se não há proxy reescrevendo o Host.
PUBLIC_WEBHOOK_URL = os.getenv("TWILIO_WEBHOOK_PUBLIC_URL", "")

# Drive webhook — token compartilhado definido em scripts/register_drive_webhook.py
DRIVE_WEBHOOK_TOKEN = os.getenv("DRIVE_WEBHOOK_TOKEN", "")


# ----------------------------------------------------------------------------
# App
# ----------------------------------------------------------------------------

app = FastAPI(
    title="SAMBA CORE — Webhook",
    description="Ingestão Twilio WhatsApp → Celery (extração + sync) + Drive RAG.",
    version="2.0.0",
)


@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    """Liveness probe — não toca DB nem Redis (esses são readiness)."""
    return "ok"


# ----------------------------------------------------------------------------
# Twilio signature validation (HMAC-SHA1)
# ----------------------------------------------------------------------------

async def verify_twilio_signature(request: Request) -> dict[str, str]:
    """
    Dependency: valida assinatura HMAC-SHA1 da Twilio e devolve o form parseado.

    Falha → 403 Forbidden. Se `TWILIO_VALIDATE_SIGNATURE=false`, pula a
    validação (apenas para desenvolvimento local).
    """
    form = await request.form()
    form_dict = {k: str(v) for k, v in form.items()}

    if not VALIDATE_SIGNATURE:
        logger.warning("TWILIO_VALIDATE_SIGNATURE=false — assinatura NÃO validada (dev mode).")
        return form_dict

    if not TWILIO_AUTH_TOKEN:
        logger.error("TWILIO_AUTH_TOKEN ausente — não posso validar assinatura.")
        raise HTTPException(status_code=500, detail="Webhook signing key não configurada.")

    signature = request.headers.get("X-Twilio-Signature", "")
    if not signature:
        logger.warning("Webhook sem header X-Twilio-Signature.")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Signature missing")

    # URL exata que a Twilio assinou — preferir a URL pública configurada.
    url = PUBLIC_WEBHOOK_URL or str(request.url)

    try:
        from twilio.request_validator import RequestValidator
    except ImportError as exc:
        logger.error("Pacote 'twilio' não instalado: %s", exc)
        raise HTTPException(status_code=500, detail="Twilio SDK indisponível.")

    validator = RequestValidator(TWILIO_AUTH_TOKEN)
    if not validator.validate(url, form_dict, signature):
        logger.warning("Assinatura Twilio inválida para url=%s", url)
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Twilio signature")

    return form_dict


# ----------------------------------------------------------------------------
# Persistência (separada para facilitar teste)
# ----------------------------------------------------------------------------

def persist_inbound_message(form: dict[str, str]) -> Optional[int]:
    """
    Insere a mensagem do Twilio na tabela `messages` e devolve seu ID.

    Idempotência: se já existe Message com mesmo `MessageSid` (gravado em
    `attachment_name` quando não há mídia, ou via lookup direto), reaproveita.

    Os campos do payload Twilio mais comuns:
      - From            ex.: "whatsapp:+5511999990001"
      - To              ex.: "whatsapp:+5513999990001"  (chip dedicado)
      - Body            texto da mensagem
      - ProfileName     nome exibido no WhatsApp
      - MessageSid      ID único (chave de idempotência)
      - NumMedia        quantidade de anexos
      - MediaUrl0..N    URLs assinadas dos anexos
      - MediaContentType0..N
    """
    from models.database import Message, get_session

    sender = form.get("From", "").replace("whatsapp:", "").strip()
    body = form.get("Body", "")
    sid = form.get("MessageSid", "").strip()
    profile = form.get("ProfileName", "").strip()
    num_media = int(form.get("NumMedia", "0") or 0)

    session = get_session()
    try:
        # Idempotência: detecta retry da Twilio pelo MessageSid (gravamos em notes
        # via campo `attachment_name` no shape atual — única coluna de string livre
        # presente na tabela `messages`). TODO real: adicionar `external_id` indexado.
        if sid:
            existing = (
                session.query(Message)
                .filter(Message.attachment_name == f"twilio:{sid}")
                .first()
            )
            if existing is not None:
                logger.info("persist_inbound_message: replay de MessageSid=%s — msg_id=%s", sid, existing.id)
                return existing.id

        msg = Message(
            timestamp=datetime.utcnow(),
            sender=sender or (profile or "unknown"),
            content=body,
            group_name=None,
            is_media=num_media > 0,
            is_system=False,
            has_attachments=num_media > 0,
            attachment_name=f"twilio:{sid}" if sid else None,
            attachment_mime_type=form.get("MediaContentType0") if num_media else None,
            attachment_data=None,  # Download de mídia fica para a task (HTTP autenticado).
            has_quote=False,        # Quem decide é o ExtractorAgent — não inferimos aqui.
        )
        session.add(msg)
        session.commit()
        session.refresh(msg)
        logger.info(
            "persist_inbound_message: msg_id=%s sender=%s media=%s sid=%s",
            msg.id, sender, num_media, sid,
        )
        return msg.id
    except Exception:
        session.rollback()
        logger.exception("persist_inbound_message: falha ao gravar Message")
        raise
    finally:
        session.close()


# ----------------------------------------------------------------------------
# Follow-Up response matching
# ----------------------------------------------------------------------------

def match_followup_response(sender: str, body: str) -> list[int]:
    """
    Verifica se o remetente `sender` (E.164, ex.: '+5511999990001') tem algum
    FollowUp com status='enviado'. Se sim, seta response_received=True e
    response_content no banco e devolve a lista de followup_ids afetados.

    Matching: compara o telefone normalizado (apenas dígitos) para tolerar
    variações de formatação entre o que está em target_person e o que chega
    do Twilio.

    Retorna lista de followup_ids que foram marcados com resposta.
    """
    from models.database import FollowUp, get_session

    # Normaliza: apenas dígitos (descarta +, espaços, traços)
    def _digits(phone: str) -> str:
        return "".join(c for c in (phone or "") if c.isdigit())

    sender_digits = _digits(sender)
    if not sender_digits:
        return []

    session = get_session()
    affected: list[int] = []
    try:
        pending = (
            session.query(FollowUp)
            .filter(FollowUp.status == "enviado")
            .all()
        )
        for fu in pending:
            # Compara contra target_person (número do contato) e target_group
            fu_digits = _digits(fu.target_person or fu.target_group or "")
            if fu_digits and fu_digits == sender_digits:
                fu.response_received = True
                fu.response_content = body[:2000]  # trunca para evitar blobs imensos
                affected.append(fu.id)
                logger.info(
                    "match_followup_response: FollowUp#%s marcado como respondido sender=%s",
                    fu.id, sender,
                )

        if affected:
            session.commit()
    except Exception:
        session.rollback()
        logger.exception("match_followup_response: erro ao atualizar follow-ups")
    finally:
        session.close()

    return affected


# ----------------------------------------------------------------------------
# Endpoint
# ----------------------------------------------------------------------------

@app.post("/webhook/twilio", status_code=status.HTTP_200_OK)
async def webhook_twilio(form: dict[str, str] = Depends(verify_twilio_signature)) -> Response:
    """
    Endpoint do Twilio WhatsApp:
      1. Valida assinatura (dependência).
      2. Persiste a Message bruta no DB.
      3. Despacha o workflow Celery (extract → sync) em fila assíncrona.
      4. Devolve TwiML vazio em < 100ms.

    Não bloqueia — toda lógica pesada (LLM, planilha, Drive) acontece nos
    workers Celery, isolada por fila (queue_extractor, queue_sync).
    """
    msg_id = persist_inbound_message(form)
    if msg_id is None:
        # Não devemos chegar aqui sem exceção — defesa em profundidade.
        logger.error("webhook_twilio: persist_inbound_message devolveu None")
        raise HTTPException(status_code=500, detail="Falha ao persistir mensagem.")

    # ── Follow-Up response matching ───────────────────────────────────────
    # Antes de rotear para o Extractor, verificamos se o remetente está
    # aguardando resposta de algum FollowUp enviado. Se sim, marcamos no banco
    # e despachamos a task de notificação interna (queue_notify, rápida).
    sender = form.get("From", "").replace("whatsapp:", "").strip()
    body   = form.get("Body", "")
    fu_ids = match_followup_response(sender, body)
    if fu_ids:
        from tasks.agent_tasks import task_process_followup_response
        for fu_id in fu_ids:
            task_process_followup_response.delay(fu_id)
            logger.info("webhook_twilio: FollowUp#%s response task dispatched", fu_id)

    # ── Detecção de áudio para ATA de reunião ────────────────────────────────
    # Se a mensagem contiver áudio (ogg, mpeg, mp4) despacha task de transcrição
    # multimodal via Gemini. Isso ocorre EM PARALELO ao extractor normal.
    num_media   = int(form.get("NumMedia", "0") or 0)
    media_url0  = form.get("MediaUrl0", "")
    media_mime0 = (form.get("MediaContentType0") or "").lower()
    _AUDIO_MIMES = ("audio/ogg", "audio/mpeg", "audio/mp4", "audio/wav", "audio/webm", "audio/x-m4a")
    if num_media > 0 and media_url0 and any(media_mime0.startswith(m) for m in _AUDIO_MIMES):
        from tasks.agent_tasks import task_process_voice_meeting_minutes
        voice_result = task_process_voice_meeting_minutes.delay(
            message_id=msg_id,
            media_url=media_url0,
            mime_type=media_mime0 or "audio/ogg",
            sender=sender,
            group=form.get("GroupName", form.get("To", "")),
        )
        logger.info(
            "webhook_twilio: áudio detectado — voice_ata task=%s mime=%s msg_id=%s",
            voice_result.id, media_mime0, msg_id,
        )

    # ── Detecção de @mention → Intelligence Router ───────────────────────────
    # Se o corpo contiver @samba / @agente / @ia / @bot, extrai a pergunta
    # e dispara o cascade de inteligência (DB → RAG → Gemini escalonado).
    # Roda em PARALELO ao extractor normal — não bloqueia a ingestão.
    try:
        from agents.whatsapp_intelligence_router import extract_question
        _mention_question = extract_question(body)
        if _mention_question:
            from tasks.agent_tasks import task_process_mention
            _mention_result = task_process_mention.delay(
                message_id=msg_id,
                sender=sender,
                group=form.get("GroupName", form.get("To", "")),
                question=_mention_question,
            )
            logger.info(
                "webhook_twilio: @mention detectado — task=%s question='%s'",
                _mention_result.id, _mention_question[:60],
            )
    except Exception as _mention_err:
        logger.warning("webhook_twilio: @mention dispatch error — %s", _mention_err)

    # ── Detecção de novo grupo "Samba x [Nome]" ──────────────────────────────
    # Quando a primeira mensagem de um grupo com esse padrão chega, disparamos:
    #   1. Mensagem de boas-vindas bilíngue NO GRUPO DO CLIENTE
    #   2. Alerta interno com checklist NCNDA NO GRUPO INTERNO
    # NUNCA: checklists ou alertas operacionais no grupo do cliente.
    try:
        _group_name = form.get("GroupName", "")
        if _group_name:
            from services.whatsapp_group_welcome import is_samba_client_group
            if is_samba_client_group(_group_name):
                from tasks.agent_tasks import task_handle_new_samba_group
                # Verifica se é primeira mensagem desse grupo (detecção de novo)
                from models.database import Message as _Msg, get_session as _gs
                _sess = _gs()
                try:
                    _prev_count = _sess.query(_Msg).filter(
                        _Msg.group_name == _group_name
                    ).count()
                    _is_new = (_prev_count <= 1)  # <=1 pois a atual já foi salva
                finally:
                    _sess.close()
                if _is_new:
                    task_handle_new_samba_group.delay(
                        group_name=_group_name,
                        group_id=form.get("To", ""),
                    )
                    logger.info(
                        "webhook_twilio: novo grupo Samba x detectado='%s' — welcome task disparada",
                        _group_name,
                    )
    except Exception as _grp_err:
        logger.warning("webhook_twilio: group welcome dispatch error — %s", _grp_err)

    # Dispatch assíncrono — import local evita acoplar a app FastAPI ao broker
    # em tempo de import (útil para testes que não querem Redis).
    from tasks.agent_tasks import task_process_inbound_message

    async_result = task_process_inbound_message.delay(msg_id)
    logger.info("webhook_twilio dispatched task=%s msg_id=%s", async_result.id, msg_id)

    # TwiML vazio = aceita, sem auto-resposta.
    twiml = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'
    return Response(content=twiml, media_type="application/xml")


# ----------------------------------------------------------------------------
# Drive Push Notification endpoint
# ----------------------------------------------------------------------------

@app.post("/webhook/drive", status_code=status.HTTP_200_OK)
async def webhook_drive(request: Request) -> Response:
    """
    Recebe Push Notifications da Google Drive API (Changes.watch).

    Protocolo Google:
      - A 1ª notificação tem X-Goog-Resource-State: "sync" — é apenas o handshake,
        não indica nenhum ficheiro alterado. Respondemos 200 e paramos.
      - Notificações subsequentes têm state "change" — indicam que há mudanças
        desde o StartPageToken. O corpo é vazio; os detalhes devem ser buscados
        via Changes.list().

    Fast-Ack: retornamos HTTP 200 imediatamente e despachamos a task Celery.
    O Google considera falha se não respondemos em 60s — nunca fazemos I/O
    pesado (Drive, LLM, DB) neste handler.

    Segurança:
      - Se DRIVE_WEBHOOK_TOKEN estiver setado (recomendado), validamos o header
        X-Goog-Channel-Token. Requests sem o token correto recebem 403.
      - Sem token configurado, apenas logamos um aviso (mode dev).
    """
    resource_state = request.headers.get("X-Goog-Resource-State", "")
    channel_id     = request.headers.get("X-Goog-Channel-Id", "")
    channel_token  = request.headers.get("X-Goog-Channel-Token", "")
    resource_id    = request.headers.get("X-Goog-Resource-Id", "")

    # ── Validação do token compartilhado ──────────────────────────────────────
    if DRIVE_WEBHOOK_TOKEN:
        if channel_token != DRIVE_WEBHOOK_TOKEN:
            logger.warning(
                "webhook_drive: token inválido channel_id=%s — rejeitando",
                channel_id,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid channel token",
            )
    else:
        logger.warning(
            "webhook_drive: DRIVE_WEBHOOK_TOKEN não configurado — "
            "aceitando sem validação (modo dev)."
        )

    # ── Handshake inicial ─────────────────────────────────────────────────────
    if resource_state == "sync":
        logger.info(
            "webhook_drive: handshake recebido channel_id=%s — OK",
            channel_id,
        )
        return Response(status_code=status.HTTP_200_OK)

    # ── Notificação real de mudança ───────────────────────────────────────────
    if resource_state not in ("change", "update", "add"):
        logger.debug(
            "webhook_drive: resource_state='%s' ignorado channel_id=%s",
            resource_state, channel_id,
        )
        return Response(status_code=status.HTTP_200_OK)

    logger.info(
        "webhook_drive: mudança detectada channel_id=%s resource_id=%s state=%s — despachando task",
        channel_id, resource_id, resource_state,
    )

    # Fast-Ack: despacha task Celery e retorna imediatamente.
    from tasks.agent_tasks import task_ingest_drive_files
    async_result = task_ingest_drive_files.delay(full_scan=False)
    logger.info("webhook_drive: task despachada task_id=%s", async_result.id)

    return Response(status_code=status.HTTP_200_OK)
