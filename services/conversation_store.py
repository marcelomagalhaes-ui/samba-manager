"""
services/conversation_store.py
==============================
CRUD da tabela ConversationHistory — isolado da UI e do motor LLM.

Por que este modulo existe:
  - O Streamlit nao deve falar SQLAlchemy cru dentro dos callbacks — isso
    vaza abstracao e complica o teste.
  - O motor LLM (`gemini_api.chat_with_tools`) recebe `history` no formato
    nativo do Gemini (`{"role": ..., "parts": [...]}`). O DB guarda no
    formato Chat Completions (role + content + tool_calls). Este modulo
    e a unica ponte oficial entre os dois formatos.
  - Encapsular aqui tambem significa: se amanha trocarmos SQLite por Postgres
    gerenciado, so esta camada muda.

Formatos:
  - DB (SQLAlchemy):          role, content: str | None, tool_calls: list | None
  - Gemini (chat_with_tools): {"role": "user"|"model", "parts": [{"text"|"function_call"|"function_response"}]}
"""
from __future__ import annotations

import logging
from typing import Any

from models.database import ConversationHistory, get_session

logger = logging.getLogger("samba.conversation")


# ----------------------------------------------------------------------------
# Persistencia
# ----------------------------------------------------------------------------

def append_turn(
    session_id: str,
    role: str,
    content: str | None = None,
    tool_calls: list[dict[str, Any]] | None = None,
) -> int:
    """
    Apensa um turno ao historico da sessao. Retorna o ID criado.

    `role` e um dos: "user", "assistant", "tool", "system".
    `tool_calls` e uma lista de dicts serializaveis (schema livre — o
    consumidor decide). Fica em coluna JSON nativa.
    """
    if role not in {"user", "assistant", "tool", "system"}:
        raise ValueError(f"role invalido: {role!r}")

    session = get_session()
    try:
        row = ConversationHistory(
            session_id=session_id,
            role=role,
            content=content,
            tool_calls=tool_calls,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        logger.debug("append_turn session=%s role=%s id=%s", session_id, role, row.id)
        return row.id
    finally:
        session.close()


def load_session(session_id: str, limit: int = 200) -> list[dict[str, Any]]:
    """
    Carrega os ultimos `limit` turnos da sessao em ordem cronologica.
    Formato de saida alinhado com o DB (nao o Gemini).
    """
    session = get_session()
    try:
        rows = (
            session.query(ConversationHistory)
            .filter(ConversationHistory.session_id == session_id)
            .order_by(ConversationHistory.timestamp.asc())
            .limit(limit)
            .all()
        )
        return [
            {
                "id": r.id,
                "role": r.role,
                "content": r.content,
                "tool_calls": r.tool_calls,
                "timestamp": r.timestamp.isoformat() if r.timestamp else None,
            }
            for r in rows
        ]
    finally:
        session.close()


# ----------------------------------------------------------------------------
# Tradutores DB <-> Gemini
# ----------------------------------------------------------------------------

def db_history_to_gemini(db_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Converte o historico persistido (DB) para o formato nativo do Gemini
    aceito por `gemini_api.chat_with_tools`.

    Regras:
      - user     -> role="user",  parts=[{"text": content}]
      - assistant ->
            se tool_calls: role="model", parts= [{"function_call": ...}, opcional {"text": ...}]
            se so texto:   role="model", parts=[{"text": content}]
      - tool     -> role="user",  parts=[{"function_response": {...}}]
      - system   -> ignorado (ja vai no system_instruction)
    """
    out: list[dict[str, Any]] = []
    for r in db_rows:
        role = r.get("role")
        content = r.get("content") or ""
        tool_calls = r.get("tool_calls")

        if role == "user":
            out.append({"role": "user", "parts": [{"text": content}]})
        elif role == "assistant":
            parts: list[dict[str, Any]] = []
            if tool_calls:
                for tc in tool_calls:
                    parts.append({"function_call": {"name": tc["name"], "args": tc.get("args", {})}})
            if content:
                parts.append({"text": content})
            if parts:
                out.append({"role": "model", "parts": parts})
        elif role == "tool":
            # `content` carrega JSON serializado do resultado; `tool_calls[0]` o nome.
            if tool_calls:
                tc = tool_calls[0]
                out.append({
                    "role": "user",
                    "parts": [{
                        "function_response": {
                            "name": tc["name"],
                            "response": tc.get("result", {}),
                        },
                    }],
                })
        # system: skip (ja aplicado no nivel do gemini_api)
    return out


def persist_assistant_turn(
    session_id: str,
    text: str,
    tool_calls_trace: list[dict[str, Any]] | None,
) -> None:
    """
    Persiste o turno do assistant e, se houve tool_calls, tambem um turno
    'tool' por ferramenta executada (para auditoria + replay fiel do
    contexto em sessoes futuras).
    """
    append_turn(
        session_id=session_id,
        role="assistant",
        content=text or None,
        tool_calls=[{"name": tc["name"], "args": tc["args"]} for tc in (tool_calls_trace or [])] or None,
    )
    for tc in tool_calls_trace or []:
        append_turn(
            session_id=session_id,
            role="tool",
            content=None,
            tool_calls=[{"name": tc["name"], "result": tc.get("result")}],
        )
