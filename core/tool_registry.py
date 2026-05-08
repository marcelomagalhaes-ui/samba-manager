"""
core/tool_registry.py
=====================
Registry de ferramentas (function calling) para o futuro Conversational Hub.

Cada tool é uma função Python registrada com:
  - `name`        — identificador estável (usado pela LLM)
  - `description` — texto que a LLM lê para decidir quando chamar
  - `parameters`  — JSON Schema dos argumentos
  - `handler`     — a função Python que executa a ação

O registry expõe a lista no formato esperado pelos provedores:
  - Google Gemini  → `to_gemini_declarations()`  (FunctionDeclaration[])
  - OpenAI/Anthr.  → `to_openai_tools()`         (tools[])

E executa a tool real com `execute(name, **kwargs)` — útil quando a LLM
devolve um `function_call` que precisa ser despachado.

Tools stub registradas (já com persistência real no DB):
  - `create_deal`         → cria Deal no pipeline
  - `update_deal_stage`   → muda etapa do Deal
  - `send_followup`       → agenda FollowUp
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

logger = logging.getLogger("samba.tools")


# ----------------------------------------------------------------------------
# Estruturas
# ----------------------------------------------------------------------------

@dataclass(frozen=True)
class ToolSpec:
    """Manifesto imutável de uma tool — schema + handler."""
    name: str
    description: str
    parameters: dict[str, Any]
    handler: Callable[..., Any]

    def to_gemini_declaration(self) -> dict[str, Any]:
        """Formato `FunctionDeclaration` da Google GenAI."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
        }

    def to_openai_tool(self) -> dict[str, Any]:
        """Formato `tools[]` do OpenAI Chat Completions / Anthropic."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class ToolRegistry:
    """Container thread-unsafe (single worker) de tools registradas."""

    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def register(
        self,
        name: str,
        description: str,
        parameters: dict[str, Any],
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator: `@registry.register(name=..., description=..., parameters=...)`."""
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            if name in self._tools:
                raise ValueError(f"Tool '{name}' já registrada.")
            self._tools[name] = ToolSpec(
                name=name,
                description=description,
                parameters=parameters,
                handler=fn,
            )
            logger.debug("Tool registrada: %s", name)
            return fn
        return decorator

    def get(self, name: str) -> ToolSpec:
        if name not in self._tools:
            raise KeyError(f"Tool '{name}' não registrada.")
        return self._tools[name]

    def list_names(self) -> list[str]:
        return sorted(self._tools.keys())

    def execute(self, name: str, **kwargs: Any) -> Any:
        """Despacha uma chamada de tool decidida pela LLM."""
        spec = self.get(name)
        logger.info("Tool execute name=%s kwargs_keys=%s", name, list(kwargs.keys()))
        return spec.handler(**kwargs)

    def to_gemini_declarations(self) -> list[dict[str, Any]]:
        return [t.to_gemini_declaration() for t in self._tools.values()]

    def to_openai_tools(self) -> list[dict[str, Any]]:
        return [t.to_openai_tool() for t in self._tools.values()]


# ----------------------------------------------------------------------------
# Instância global + tools stub iniciais
# ----------------------------------------------------------------------------

registry = ToolRegistry()


@registry.register(
    name="create_deal",
    description=(
        "Cria um novo Deal no pipeline comercial da Samba Export a partir dos "
        "dados extraídos de uma cotação. Use quando o usuário descrever uma "
        "negociação concreta (commodity + direção compra/venda)."
    ),
    parameters={
        "type": "object",
        "properties": {
            "commodity": {"type": "string", "description": "Commodity (SOJA, MILHO, AÇÚCAR, FRANGO, ...)."},
            "direcao": {"type": "string", "enum": ["BID", "ASK"], "description": "BID = compra; ASK = venda."},
            "volume": {"type": "number"},
            "volume_unit": {"type": "string", "default": "MT"},
            "price": {"type": "number"},
            "currency": {"type": "string", "default": "USD"},
            "incoterm": {"type": "string"},
            "origin": {"type": "string"},
            "destination": {"type": "string"},
            "source_group": {"type": "string"},
            "source_sender": {"type": "string"},
            "name": {"type": "string", "description": "Nome do deal (auto-gerado se omitido)."},
        },
        "required": ["commodity", "direcao"],
    },
)
def create_deal(**fields: Any) -> dict[str, Any]:
    """Persiste um Deal e devolve o ID criado + estágio inicial."""
    from models.database import Deal, get_session
    session = get_session()
    try:
        if not fields.get("name"):
            fields["name"] = f"{fields['direcao']} {fields['commodity']} {datetime.utcnow():%Y%m%d-%H%M%S}"
        deal = Deal(**fields)
        session.add(deal)
        session.commit()
        return {"deal_id": deal.id, "name": deal.name, "stage": deal.stage}
    finally:
        session.close()


@registry.register(
    name="update_deal_stage",
    description=(
        "Atualiza o estágio de um Deal existente no pipeline. Use quando "
        "houver progressão (Qualificação → Negociação → Fechado) ou perda."
    ),
    parameters={
        "type": "object",
        "properties": {
            "deal_id": {"type": "integer"},
            "stage": {
                "type": "string",
                "description": "Lead Capturado | Qualificação | Negociação | Fechado | Perdido",
            },
            "notes": {"type": "string", "description": "Anotação livre apensada ao histórico do deal."},
        },
        "required": ["deal_id", "stage"],
    },
)
def update_deal_stage(deal_id: int, stage: str, notes: str | None = None) -> dict[str, Any]:
    """Move um Deal de estágio. Levanta `ValueError` se o deal não existir."""
    from models.database import Deal, get_session
    session = get_session()
    try:
        deal = session.query(Deal).filter(Deal.id == deal_id).one_or_none()
        if deal is None:
            raise ValueError(f"Deal {deal_id} não encontrado")
        deal.stage = stage
        if notes:
            deal.notes = (deal.notes or "") + f"\n[{datetime.utcnow():%Y-%m-%d %H:%M}] {stage}: {notes}"
        session.commit()
        return {"deal_id": deal_id, "stage": stage}
    finally:
        session.close()


@registry.register(
    name="send_followup",
    description=(
        "Agenda um follow-up para um contato/grupo (WhatsApp). Use quando "
        "uma resposta é esperada após um prazo (ex.: 'cobrar resposta amanhã')."
    ),
    parameters={
        "type": "object",
        "properties": {
            "deal_id": {"type": "integer"},
            "target_person": {"type": "string"},
            "target_group": {"type": "string"},
            "message": {"type": "string", "description": "Texto do follow-up que será enviado."},
            "due_in_hours": {"type": "integer", "default": 24},
        },
        "required": ["target_person", "message"],
    },
)
def send_followup(
    target_person: str,
    message: str,
    deal_id: int | None = None,
    target_group: str | None = None,
    due_in_hours: int = 24,
) -> dict[str, Any]:
    """Cria um FollowUp pendente — o despacho real fica para o FollowUp scheduler."""
    from models.database import FollowUp, get_session
    session = get_session()
    try:
        fu = FollowUp(
            deal_id=deal_id,
            target_person=target_person,
            target_group=target_group,
            message=message,
            due_at=datetime.utcnow() + timedelta(hours=due_in_hours),
            status="pendente",
        )
        session.add(fu)
        session.commit()
        return {"followup_id": fu.id, "due_at": fu.due_at.isoformat()}
    finally:
        session.close()


@registry.register(
    name="request_missing_info",
    description=(
        "Verifica quais campos críticos estão faltando em um Deal e agenda uma "
        "mensagem WhatsApp ao remetente pedindo os dados em falta. Use quando "
        "o deal estiver em 'Qualificação' ou quando o usuário pedir para cobrar "
        "informações de um contato."
    ),
    parameters={
        "type": "object",
        "properties": {
            "deal_id": {"type": "integer", "description": "ID do Deal no pipeline."},
            "due_in_minutes": {
                "type": "integer",
                "default": 30,
                "description": "Minutos até o envio do follow-up.",
            },
        },
        "required": ["deal_id"],
    },
)
def request_missing_info(deal_id: int, due_in_minutes: int = 30) -> dict[str, Any]:
    """
    Lê os campos do Deal, identifica o que falta, gera a mensagem de cobrança
    em português natural e agenda o FollowUp.
    """
    from models.database import Deal, FollowUp, get_session

    session = get_session()
    try:
        deal = session.query(Deal).filter(Deal.id == deal_id).one_or_none()
        if deal is None:
            return {"error": f"Deal {deal_id} não encontrado."}

        missing: list[str] = []
        commodity = deal.commodity or ""
        if not commodity or commodity.lower() in ("indefinida", "indefinido"):
            missing.append("Produto/Especificação")
        if not deal.volume:
            missing.append("Volume (MT/Sacas)")
        if not deal.incoterm:
            missing.append("Incoterm (FOB/CIF/etc)")
        if deal.direcao == "BID" and not deal.price:
            missing.append("Target Price")
        if deal.incoterm in ("CIF", "CFR") and not deal.destination:
            missing.append("Porto de Destino (Necessário para CIF)")

        if not missing:
            return {"status": "completo", "deal_id": deal_id, "message": "Nenhum campo faltante."}

        # Notifica o time interno (email + WhatsApp corporativo)
        try:
            from services.internal_notify import get_notifier
            notify_result = get_notifier().alert_missing_fields(
                deal_id=deal_id,
                deal_name=deal.name,
                commodity=commodity,
                assignee=deal.assignee or "Leonardo",
                source_sender=deal.source_sender or "",
                source_group=deal.source_group or "",
                missing=missing,
            )
        except Exception as exc:
            logger.warning("request_missing_info: falha na notificação interna — %s", exc)
            notify_result = {"email": False, "whatsapp": False}

        # Registra FollowUp como rastreador de pendência (target = assignee interno)
        fu = FollowUp(
            deal_id=deal_id,
            target_person=deal.assignee or "Leonardo",
            target_group=deal.source_group or "",
            message=f"[Pendência] {deal.name} — Faltam: {', '.join(missing)}",
            due_at=datetime.utcnow() + timedelta(minutes=due_in_minutes),
            status="pendente",
        )
        session.add(fu)
        session.commit()
        logger.info("request_missing_info: followup_id=%s deal=%s faltam=%s", fu.id, deal_id, missing)
        return {
            "followup_id": fu.id,
            "deal_id": deal_id,
            "assignee": deal.assignee or "Leonardo",
            "missing_fields": missing,
            "notificacao_email": notify_result.get("email"),
            "notificacao_whatsapp": notify_result.get("whatsapp"),
            "due_at": fu.due_at.isoformat(),
        }
    finally:
        session.close()
