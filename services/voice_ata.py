"""
services/voice_ata.py
=====================
Processamento de áudio de reunião → Ata Executiva + Action Items.

Fluxo:
  1. Download do áudio via URL autenticada do Twilio (MediaUrl0).
  2. Envio ao Gemini 1.5 Pro (multimodal) com prompt estruturado.
  3. Parsing da resposta: texto da ATA + JSON de action_items.
  4. Persistência dos action items na tabela meeting_action_items.
  5. Retorna dict com ata_text, action_items, raw.

Env vars:
  TWILIO_ACCOUNT_SID  — para download autenticado do áudio
  TWILIO_AUTH_TOKEN   — idem
  GEMINI_API_KEY      — modelo Gemini

Modelos suportados:
  gemini-1.5-pro   — melhor qualidade multimodal (padrão)
  gemini-2.0-flash — mais rápido, menor qualidade de transcrição
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger("samba.voice_ata")

AUDIO_MODEL = os.getenv("VOICE_ATA_MODEL", "gemini-1.5-pro")

# ── Prompt executivo ─────────────────────────────────────────────────────────
_ATA_PROMPT = """Você é o secretário executivo da Samba Export — empresa de trading de commodities agrícolas (soja, milho, açúcar, farelo, frango, etc.).

Você recebeu a gravação de uma reunião ou conversa de negócios em português.

Gere:

## ATA EXECUTIVA — {date}

**Participantes identificados:** [extraia dos áudios ou use "não identificados"]
**Duração estimada:** [se possível]

### Decisões Tomadas
[Liste todas as decisões, acordos e definições objetivamente]

### Pontos Discutidos
[Principais tópicos, em bullets]

### Contexto Comercial
[Commodities mencionadas, volumes, preços, destinos, parceiros — se houver]

---

## ACTION ITEMS

Identifique TODAS as tarefas, compromissos e pendências. Frases indicativas:
- "Fulano, manda o contrato até sexta"
- "Precisamos pedir o LOI para o Gui"
- "Tem que ligar para o fornecedor"
- "Vamos marcar reunião com o cliente"
- "Não esqueçam de..."

Responda o JSON abaixo (OBRIGATÓRIO — mesmo que vazio):

```json
{{
  "action_items": [
    {{
      "responsible": "Nome ou cargo (ex: Leonardo, Nivio, Marcelo, Equipe Comercial, Indefinido)",
      "action": "Descrição clara e acionável da tarefa",
      "priority": "critica|alta|media|baixa",
      "due_date": "YYYY-MM-DD ou null"
    }}
  ]
}}
```

Seja preciso, executivo e direto. Prioridade critica = impacto financeiro imediato ou prazo < 48h."""


def download_audio(media_url: str) -> bytes:
    """
    Baixa áudio do Twilio MediaUrl usando credenciais básicas.

    Raises:
        requests.HTTPError: se o download falhar (4xx/5xx).
    """
    account_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
    auth_token  = os.getenv("TWILIO_AUTH_TOKEN", "")

    if not account_sid or not auth_token:
        logger.error("download_audio: TWILIO_ACCOUNT_SID / AUTH_TOKEN não configurados")
        raise RuntimeError("Credenciais Twilio ausentes para download de mídia.")

    resp = requests.get(media_url, auth=(account_sid, auth_token), timeout=60)
    resp.raise_for_status()
    logger.info("download_audio: %d bytes baixados mime=%s",
                len(resp.content), resp.headers.get("Content-Type", "?"))
    return resp.content


def _parse_action_items(raw_text: str) -> list[dict]:
    """Extrai o bloco JSON de action items do texto gerado pelo Gemini."""
    match = re.search(r"```json\s*(\{.*?\})\s*```", raw_text, re.DOTALL)
    if not match:
        # Fallback: tenta parse direto se o JSON estiver solto
        match = re.search(r'\{"action_items".*?\}(?=\s*$|\s*##)', raw_text, re.DOTALL)
    if not match:
        logger.warning("_parse_action_items: bloco JSON não encontrado na resposta")
        return []
    try:
        parsed = json.loads(match.group(1))
        items = parsed.get("action_items", [])
        logger.info("_parse_action_items: %d action items extraídos", len(items))
        return items
    except json.JSONDecodeError as exc:
        logger.warning("_parse_action_items: JSON inválido — %s", exc)
        return []


def process_audio_to_ata(
    audio_data: bytes,
    mime_type: str = "audio/ogg",
    sender: str = "",
    group: str = "",
) -> dict:
    """
    Envia áudio ao Gemini multimodal e retorna ATA + action items parseados.

    Args:
        audio_data: bytes do arquivo de áudio
        mime_type:  MIME type (ex.: audio/ogg, audio/mpeg, audio/mp4)
        sender:     remetente WPP (para contexto)
        group:      grupo WPP de origem

    Returns:
        {
          "ata_text":     str  — ATA formatada sem o bloco JSON,
          "action_items": list[dict],
          "raw":          str  — resposta completa do Gemini,
          "sender":       str,
          "group":        str,
          "processed_at": str (ISO),
        }
    """
    import google.generativeai as genai

    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não configurada.")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(AUDIO_MODEL)

    date_str = datetime.utcnow().strftime("%d/%m/%Y %H:%M UTC")
    prompt   = _ATA_PROMPT.format(date=date_str)

    # Inline base64 para áudios ≤ 20 MB (Gemini inline data API)
    audio_b64 = base64.standard_b64encode(audio_data).decode("utf-8")

    logger.info(
        "process_audio_to_ata: enviando %d bytes ao %s mime=%s sender=%s group=%s",
        len(audio_data), AUDIO_MODEL, mime_type, sender, group,
    )

    response = model.generate_content([
        {"mime_type": mime_type, "data": audio_b64},
        prompt,
    ])
    full_text = (response.text or "").strip()

    # Separa ATA limpa do bloco JSON
    ata_clean = re.sub(r"```json.*?```", "", full_text, flags=re.DOTALL).strip()
    action_items = _parse_action_items(full_text)

    return {
        "ata_text":     ata_clean,
        "action_items": action_items,
        "raw":          full_text,
        "sender":       sender,
        "group":        group,
        "processed_at": datetime.utcnow().isoformat(),
    }


def persist_action_items(
    action_items: list[dict],
    message_id: Optional[int],
    source_group: str,
    ata_snippet: str,
) -> list[int]:
    """
    Persiste os action items extraídos em meeting_action_items.

    Returns:
        Lista de IDs criados.
    """
    if not action_items:
        return []

    from models.database import MeetingActionItem, get_session

    ids: list[int] = []
    session = get_session()
    try:
        for item in action_items:
            # Parse de due_date
            due_dt: Optional[datetime] = None
            raw_due = (item.get("due_date") or "").strip()
            if raw_due and raw_due.lower() not in ("null", "none", ""):
                try:
                    due_dt = datetime.strptime(raw_due[:10], "%Y-%m-%d")
                except ValueError:
                    pass

            row = MeetingActionItem(
                message_id   = message_id,
                responsible  = (item.get("responsible") or "Indefinido")[:200],
                action       = (item.get("action") or "")[:2000],
                priority     = (item.get("priority") or "media")[:20],
                status       = "pendente",
                due_date     = due_dt,
                ata_snippet  = ata_snippet[:500] if ata_snippet else None,
                source_group = source_group[:200] if source_group else None,
            )
            session.add(row)
            session.flush()
            ids.append(row.id)

        session.commit()
        logger.info("persist_action_items: %d itens persistidos ids=%s", len(ids), ids)
    except Exception:
        session.rollback()
        logger.exception("persist_action_items: erro ao persistir")
        raise
    finally:
        session.close()

    return ids


def format_ata_for_wpp(result: dict) -> str:
    """
    Formata a ATA e os action items para envio no WhatsApp interno.
    Mantém mensagem abaixo de ~3000 chars para evitar truncagem.
    """
    ata   = (result.get("ata_text") or "")[:2000]
    items = result.get("action_items") or []
    ts    = result.get("processed_at", "")[:16].replace("T", " ")

    priority_icon = {"critica": "🔴", "alta": "🟠", "media": "🟡", "baixa": "🟢"}

    lines = [
        "📋 *ATA DE REUNIÃO — SAMBA EXPORT*",
        f"_{ts} UTC · {result.get('group', 'WhatsApp')}_",
        "",
        ata[:1800],
        "",
    ]

    if items:
        lines.append(f"✅ *ACTION ITEMS ({len(items)})*")
        for i, it in enumerate(items, 1):
            icon = priority_icon.get((it.get("priority") or "media").lower(), "•")
            due  = f" · prazo {it['due_date']}" if it.get("due_date") else ""
            resp = it.get("responsible") or "Indefinido"
            lines.append(f"{icon} *{i}. {resp}*{due}")
            lines.append(f"   {it.get('action', '')[:200]}")
    else:
        lines.append("_Nenhum action item identificado._")

    lines.append("")
    lines.append("_Gerado por Samba Voice ATA · Gemini Multimodal_")
    return "\n".join(lines)
