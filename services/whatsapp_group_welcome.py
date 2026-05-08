"""
services/whatsapp_group_welcome.py
=====================================
Gerencia o fluxo de entrada de novos grupos WhatsApp "Samba x [Nome]":

  1. Detecta grupo novo (por nome ou first-message-flag)
  2. Envia mensagem de boas-vindas bilíngue PT/EN no grupo do cliente
  3. Envia alerta interno ao INTERNAL_WPP_GROUP com:
       - Quem é o lead
       - Status NCNDA: pendente + checklist de dados faltantes
       - Qualificação comercial: o que já foi capturado / o que falta

Regra de ouro:
  - Mensagem de boas-vindas → vai NO GRUPO DO CLIENTE
  - Todos os alertas, checklists, status → vão NO GRUPO INTERNO
  - NUNCA expor checklist ou avisos operacionais no grupo do cliente

Integração:
  - Chamado pelo webhook Twilio quando um grupo novo é detectado
  - Também pode ser chamado manualmente via task Celery
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
load_dotenv(override=True)

# ── Constantes ────────────────────────────────────────────────────────────────
INTERNAL_GROUP   = os.getenv("INTERNAL_WPP_GROUP", "")
SOCIO_1_NAME     = os.getenv("SOCIO_1_NAME", "Leonardo")
SOCIO_2_NAME     = os.getenv("SOCIO_2_NAME", "Nívio")
SOCIO_3_NAME     = os.getenv("SOCIO_3_NAME", "Marcelo")

# Padrão de nome de grupo cliente: "Samba x [Nome]" ou "Samba x [Nome] [Empresa]"
GROUP_NAME_PATTERN = re.compile(
    r"^samba\s+x\s+(.+)$",
    re.IGNORECASE,
)

# ── Templates de mensagem ─────────────────────────────────────────────────────

WELCOME_MESSAGE_CLIENT = """🌿 *Welcome to Samba Export* | *Bem-vindo à Samba Export*

━━━━━━━━━━━━━━━━━━━━━━
🇺🇸 *ENGLISH*
━━━━━━━━━━━━━━━━━━━━━━
We are Brazilian commodity originators and exporters — operating at scale with full compliance, from origin to shipment worldwide.

Our team ({socio1}, {socio2} & {socio3}) will be in touch shortly to understand your needs and move things forward.

Before we proceed to any commercial discussion, we will send you our *NCNDA (Non-Circumvention & Non-Disclosure Agreement)* — a standard document that protects all parties involved. It can be signed digitally in under 2 minutes.

If you have any immediate questions, feel free to type them here.

━━━━━━━━━━━━━━━━━━━━━━
🇧🇷 *PORTUGUÊS*
━━━━━━━━━━━━━━━━━━━━━━
Somos originadores e exportadores brasileiros de commodities — operando em larga escala com compliance completo, da origem ao embarque mundial.

Nossa equipe ({socio1}, {socio2} & {socio3}) entrará em contato em breve para entender suas necessidades e avançar na conversa.

Antes de qualquer discussão comercial, enviaremos nosso *NCNDA (Acordo de Não Circunvenção e Confidencialidade)* — documento padrão que protege todas as partes envolvidas. A assinatura digital leva menos de 2 minutos.

Se tiver alguma dúvida imediata, fique à vontade para escrever aqui.

━━━━━━━━━━━━━━━━━━━━━━
_🇧🇷 Samba Export — From Brazil to the World_"""


INTERNAL_ALERT_NEW_GROUP = """🔔 *NOVO GRUPO CRIADO — {group_name}*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 *Lead:* {lead_name}
📅 *Criado em:* {created_at}
📋 *Status NCNDA:* ⚠️ PENDENTE — não assinado

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 *DADOS NECESSÁRIOS PARA EMITIR O NCNDA*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

*Parte II — Intermediário Brasileiro (se houver):*
☐ Razão Social completa
☐ CNPJ
☐ Endereço completo (rua, nº, cidade, estado, CEP)
☐ Nome do Representante Legal
☐ Nacionalidade + Profissão do Rep.
☐ Nº Passaporte ou CPF/RG do Rep.

*Parte III — Empresa do Lado Comprador (estrangeira):*
☐ Nome legal completo da empresa
☐ País de constituição + regime jurídico (ex: "England and Wales")
☐ Nº de registro da empresa no país de origem
☐ Endereço completo registrado
☐ Nome do Representante Legal
☐ Profissão/cargo do Rep.
☐ Nº Passaporte do Rep.

*Para o contrato:*
☐ Data de assinatura (DD/MM/AAAA)
☐ Fórmula de penalidade (Cláusula 5.3.3) — ex: "100% da comissão devida"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 *QUALIFICAÇÃO COMERCIAL*
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{qualification_status}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📎 Modelo NCNDA: Drive → MODELOS DE DOCUMENTOS → __NCDA
📎 Versão EN disponível: Drive → MODELOS DE DOCUMENTOS → __NCDA → NCNDA_EN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_Ação requerida: coletar dados acima e emitir NCNDA antes da negociação._"""


INTERNAL_ALERT_NCDA_REMINDER = """⚠️ *LEMBRETE — NCNDA PENDENTE*

Grupo: *{group_name}*
Lead: {lead_name}
Criado há: {days_since} dia(s)

O NCNDA ainda não foi assinado.
Nenhuma proposta (FCO) deve ser enviada antes da assinatura.

_Para emitir: Drive → MODELOS DE DOCUMENTOS → __NCDA_"""


INTERNAL_ALERT_NCDA_SIGNED = """✅ *NCNDA ASSINADO — {group_name}*

Lead: *{lead_name}*
Assinado em: {signed_at}

🟢 Negociação pode avançar.
Próximo passo: FCO assim que qualificação comercial estiver completa.

Qualificação atual:
{qualification_status}"""


# ── Qualificação comercial ────────────────────────────────────────────────────

def build_qualification_status(deal_data: dict) -> str:
    """
    Monta o bloco de status de qualificação comercial para o alerta interno.
    deal_data: dict com campos do deal (pode vir do ExtractorAgent).
    """
    fields = {
        "Commodity":            deal_data.get("commodity"),
        "Volume":               deal_data.get("volume"),
        "Porto destino":        deal_data.get("destination_port"),
        "Incoterm (CIF/FOB)":   deal_data.get("incoterm"),
        "Instrumento pagamento": deal_data.get("payment_instrument"),
        "Comprador final ou mandatário": deal_data.get("buyer_type"),
        "Empresa verificável":  deal_data.get("company_verified"),
    }
    lines = []
    for label, value in fields.items():
        if value:
            lines.append(f"✅ {label}: {value}")
        else:
            lines.append(f"☐ {label}: _pendente_")
    return "\n".join(lines)


# ── Detecção de grupo ─────────────────────────────────────────────────────────

def parse_lead_name(group_name: str) -> Optional[str]:
    """
    Extrai o nome do lead do nome do grupo.
    'Samba x Tejinder MBB' → 'Tejinder MBB'
    """
    match = GROUP_NAME_PATTERN.match(group_name.strip())
    if match:
        return match.group(1).strip()
    return None


def is_samba_client_group(group_name: str) -> bool:
    """Retorna True se o nome segue o padrão 'Samba x [Nome]'."""
    return GROUP_NAME_PATTERN.match(group_name.strip()) is not None


# ── Envio de mensagens ────────────────────────────────────────────────────────

def send_welcome_to_client_group(group_id: str, group_name: str) -> bool:
    """
    Envia a mensagem de boas-vindas bilíngue no grupo do cliente.
    Retorna True se enviado com sucesso.
    """
    from services.internal_notify import send_whatsapp_message
    message = WELCOME_MESSAGE_CLIENT.format(
        socio1=SOCIO_1_NAME,
        socio2=SOCIO_2_NAME,
        socio3=SOCIO_3_NAME,
    )
    return send_whatsapp_message(to=group_id, body=message)


def send_new_group_alert_to_internal(
    group_name: str,
    group_id: str,
    deal_data: Optional[dict] = None,
) -> bool:
    """
    Envia alerta de novo grupo + checklist NCNDA ao grupo interno.
    """
    from services.internal_notify import send_internal_wpp
    lead_name    = parse_lead_name(group_name) or group_name
    created_at   = datetime.now().strftime("%d/%m/%Y %H:%M")
    qual_status  = build_qualification_status(deal_data or {})

    message = INTERNAL_ALERT_NEW_GROUP.format(
        group_name       = group_name,
        lead_name        = lead_name,
        created_at       = created_at,
        qualification_status = qual_status,
    )
    return send_internal_wpp(message)


def send_ncda_signed_alert(
    group_name: str,
    signed_at: Optional[str] = None,
    deal_data: Optional[dict] = None,
) -> bool:
    """
    Notifica o grupo interno quando o NCNDA é assinado.
    """
    from services.internal_notify import send_internal_wpp
    lead_name  = parse_lead_name(group_name) or group_name
    signed_str = signed_at or datetime.now().strftime("%d/%m/%Y %H:%M")
    qual_status = build_qualification_status(deal_data or {})

    message = INTERNAL_ALERT_NCDA_SIGNED.format(
        group_name           = group_name,
        lead_name            = lead_name,
        signed_at            = signed_str,
        qualification_status = qual_status,
    )
    return send_internal_wpp(message)


def send_ncda_reminder(group_name: str, days_since: int = 1) -> bool:
    """
    Lembrete interno: NCNDA ainda não assinado após N dias.
    """
    from services.internal_notify import send_internal_wpp
    lead_name = parse_lead_name(group_name) or group_name
    message = INTERNAL_ALERT_NCDA_REMINDER.format(
        group_name  = group_name,
        lead_name   = lead_name,
        days_since  = days_since,
    )
    return send_internal_wpp(message)


# ── Handler principal (chamado pelo webhook) ──────────────────────────────────

def handle_new_group(
    group_name: str,
    group_id: str,
    deal_data: Optional[dict] = None,
) -> dict:
    """
    Ponto de entrada principal quando um novo grupo 'Samba x' é detectado.

    Fluxo:
      1. Valida que é um grupo cliente (padrão 'Samba x')
      2. Envia boas-vindas bilíngue NO GRUPO DO CLIENTE
      3. Envia alerta + checklist NCNDA NO GRUPO INTERNO

    Returns:
      dict com status de cada ação
    """
    if not is_samba_client_group(group_name):
        return {
            "is_client_group": False,
            "welcome_sent": False,
            "internal_alert_sent": False,
            "reason": f"Nome '{group_name}' não segue padrão 'Samba x [Nome]'",
        }

    welcome_sent      = send_welcome_to_client_group(group_id, group_name)
    internal_sent     = send_new_group_alert_to_internal(group_name, group_id, deal_data)

    return {
        "is_client_group":      True,
        "lead_name":            parse_lead_name(group_name),
        "welcome_sent":         welcome_sent,
        "internal_alert_sent":  internal_sent,
    }
