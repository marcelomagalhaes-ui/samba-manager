"""
services/whatsapp_api.py
========================
Gerenciamento de múltiplas contas WhatsApp (Twilio).

Arquitetura: cada agente de IA tem seu próprio chip/número dedicado.
  - Extractor  → só lê grupos, NUNCA responde
  - Follow-Up  → lê e responde ativamente
  - Manager    → envia relatórios e alertas aos sócios
  - Documental → envia documentos e checklists
  - Agenda     → envia lembretes e agendamentos

Modo offline (WHATSAPP_OFFLINE=true) simula envios no terminal,
sem chamar a API Twilio — útil para desenvolvimento local.
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# Enums
# ──────────────────────────────────────────────

class AgentRole(str, Enum):
    EXTRACTOR  = "extractor"   # Minerador — lê grupos, nunca responde
    FOLLOWUP   = "followup"    # Follow-Up — responde ativamente
    MANAGER    = "manager"     # Gerente Geral — alertas e relatórios
    DOCUMENTAL = "documental"  # Documental / Risco — documentos
    AGENDA     = "agenda"      # Agenda / Secretariado — lembretes


# ──────────────────────────────────────────────
# Dataclass de conta
# ──────────────────────────────────────────────

@dataclass
class WhatsAppAccount:
    """
    Representa uma conta WhatsApp Business com chip dedicado.

    Attributes:
        agent_name:    Identificador humano do agente (ex: "Extractor").
        role:          Papel do agente no sistema (AgentRole).
        phone_number:  Número E.164 do chip dedicado (ex: +5513999990001).
        twilio_sid:    Account SID Twilio desta conta.
        twilio_token:  Auth Token Twilio desta conta.
        can_reply:     Se False, o agente NUNCA envia mensagens (read-only).
    """
    agent_name:   str
    role:         AgentRole
    phone_number: str
    twilio_sid:   str
    twilio_token: str
    can_reply:    bool = True

    # Cliente Twilio lazy-loaded
    _client: object = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self):
        # Extrator nunca pode responder — forçado aqui como salvaguarda
        if self.role == AgentRole.EXTRACTOR:
            object.__setattr__(self, "can_reply", False)

    @property
    def whatsapp_from(self) -> str:
        """Formato 'whatsapp:+55...' exigido pela Twilio."""
        n = self.phone_number.lstrip("+")
        return f"whatsapp:+{n}"

    def get_client(self):
        """Retorna o cliente Twilio para esta conta (lazy init)."""
        if self._client is None:
            try:
                from twilio.rest import Client
                self._client = Client(self.twilio_sid, self.twilio_token)
            except ImportError:
                raise RuntimeError(
                    "Pacote 'twilio' não instalado. Execute: pip install twilio"
                )
        return self._client

    def send_message(self, to: str, body: str, offline: bool = False) -> dict:
        """
        Envia mensagem WhatsApp para 'to' (número E.164 ou grupo SID).

        Args:
            to:      Número destino (ex: +5511999999999).
            body:    Texto da mensagem.
            offline: Se True, apenas loga sem chamar a API.

        Returns:
            dict com sid e status (ou simulado em modo offline).

        Raises:
            PermissionError: se o agente for read-only (Extractor).
        """
        if not self.can_reply:
            raise PermissionError(
                f"Agente '{self.agent_name}' é read-only e não pode enviar mensagens."
            )

        to_wa = f"whatsapp:{to}" if not to.startswith("whatsapp:") else to

        if offline:
            logger.info(
                "[OFFLINE] %s → %s: %s", self.agent_name, to_wa, body[:80]
            )
            return {"sid": "OFFLINE_SIM", "status": "simulated", "to": to_wa, "body": body}

        client = self.get_client()
        message = client.messages.create(
            from_=self.whatsapp_from,
            to=to_wa,
            body=body,
        )
        logger.info(
            "[SENT] %s → %s | sid=%s status=%s",
            self.agent_name, to_wa, message.sid, message.status,
        )
        return {"sid": message.sid, "status": message.status, "to": to_wa}


# ──────────────────────────────────────────────
# Manager de contas
# ──────────────────────────────────────────────

class WhatsAppManager:
    """
    Gerencia as 5 contas WhatsApp dedicadas dos agentes.

    Lê as env vars de cada agente e constrói os WhatsAppAccount.
    Usa Twilio Sandbox global como fallback se a conta individual
    não tiver SID/Token próprios (útil durante onboarding dos chips).

    Env vars esperadas:
        AGENT_EXTRACTOR_PHONE, AGENT_FOLLOWUP_PHONE,
        AGENT_MANAGER_PHONE, AGENT_DOCUMENTAL_PHONE, AGENT_AGENDA_PHONE

        Por conta (opcional — fallback para TWILIO_ACCOUNT_SID/TOKEN):
        AGENT_<ROLE>_TWILIO_SID, AGENT_<ROLE>_TWILIO_TOKEN

        WHATSAPP_OFFLINE=true  →  modo dev sem chamadas reais
    """

    # Mapeamento role → env var do número e role do agente
    _ROLE_CONFIG: list[tuple[AgentRole, str, str, bool]] = [
        # (role, env_phone, display_name, can_reply)
        (AgentRole.EXTRACTOR,  "AGENT_EXTRACTOR_PHONE",  "Extractor",  False),
        (AgentRole.FOLLOWUP,   "AGENT_FOLLOWUP_PHONE",   "Follow-Up",  True),
        (AgentRole.MANAGER,    "AGENT_MANAGER_PHONE",    "Manager",    True),
        (AgentRole.DOCUMENTAL, "AGENT_DOCUMENTAL_PHONE", "Documental", True),
        (AgentRole.AGENDA,     "AGENT_AGENDA_PHONE",     "Agenda",     True),
    ]

    def __init__(self):
        self.offline: bool = os.getenv("WHATSAPP_OFFLINE", "false").lower() == "true"
        self._accounts: dict[AgentRole, WhatsAppAccount] = {}
        self._load_accounts()

    def _load_accounts(self):
        """Carrega todas as contas a partir das env vars."""
        # Credenciais Twilio globais (fallback)
        global_sid   = os.getenv("TWILIO_ACCOUNT_SID", "")
        global_token = os.getenv("TWILIO_AUTH_TOKEN", "")

        for role, env_phone, display_name, can_reply in self._ROLE_CONFIG:
            phone = os.getenv(env_phone, "")

            # Credenciais por conta ou fallback global
            role_key  = role.value.upper()
            sid   = os.getenv(f"AGENT_{role_key}_TWILIO_SID",   global_sid)
            token = os.getenv(f"AGENT_{role_key}_TWILIO_TOKEN", global_token)

            if not phone:
                logger.warning(
                    "Env var %s não definida — conta '%s' desabilitada.",
                    env_phone, display_name,
                )
                phone = "+5500000000000"  # placeholder para não quebrar imports

            account = WhatsAppAccount(
                agent_name=display_name,
                role=role,
                phone_number=phone,
                twilio_sid=sid,
                twilio_token=token,
                can_reply=can_reply,
            )
            self._accounts[role] = account
            logger.debug("Conta carregada: %s (%s)", display_name, phone)

    # ── Acesso por role ──────────────────────────────────────

    def get(self, role: AgentRole) -> WhatsAppAccount:
        """Retorna a conta do agente pelo seu role."""
        return self._accounts[role]

    @property
    def extractor(self) -> WhatsAppAccount:
        return self._accounts[AgentRole.EXTRACTOR]

    @property
    def followup(self) -> WhatsAppAccount:
        return self._accounts[AgentRole.FOLLOWUP]

    @property
    def manager(self) -> WhatsAppAccount:
        return self._accounts[AgentRole.MANAGER]

    @property
    def documental(self) -> WhatsAppAccount:
        return self._accounts[AgentRole.DOCUMENTAL]

    @property
    def agenda(self) -> WhatsAppAccount:
        return self._accounts[AgentRole.AGENDA]

    # ── Envio conveniente ────────────────────────────────────

    def send(self, role: AgentRole, to: str, body: str) -> dict:
        """
        Envia mensagem pela conta do agente especificado.
        Respeita o modo offline e a regra de can_reply.
        """
        account = self.get(role)
        return account.send_message(to=to, body=body, offline=self.offline)

    def broadcast(self, recipients: list[str], body: str, role: AgentRole = AgentRole.MANAGER) -> list[dict]:
        """Envia a mesma mensagem para múltiplos destinatários."""
        return [self.send(role, to, body) for to in recipients]

    # ── Diagnóstico ──────────────────────────────────────────

    def status(self) -> list[dict]:
        """Retorna resumo do estado de todas as contas."""
        rows = []
        for role, account in self._accounts.items():
            rows.append({
                "role":         role.value,
                "agent_name":   account.agent_name,
                "phone":        account.phone_number,
                "can_reply":    account.can_reply,
                "has_sid":      bool(account.twilio_sid),
                "has_token":    bool(account.twilio_token),
                "offline_mode": self.offline,
            })
        return rows

    def __repr__(self) -> str:
        mode = "OFFLINE" if self.offline else "LIVE"
        return f"<WhatsAppManager mode={mode} accounts={len(self._accounts)}>"


# ──────────────────────────────────────────────
# Instância global (singleton lazy)
# ──────────────────────────────────────────────

_manager: Optional[WhatsAppManager] = None


def get_whatsapp_manager() -> WhatsAppManager:
    """Retorna a instância singleton do WhatsAppManager."""
    global _manager
    if _manager is None:
        _manager = WhatsAppManager()
    return _manager
