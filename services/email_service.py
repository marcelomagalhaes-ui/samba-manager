"""
services/email_service.py
=========================
Envia emails HTML via Gmail API v1, reutilizando as credenciais OAuth2
do mesmo projeto GCP já autenticado pelo DriveManager.

Remetente fixo: SAMBA_AGENT_EMAIL (env, default agente@sambaexport.com.br)
Este usuário é a identidade única do agente de email — um só endereço para
todos os tipos de notificação (alertas intraday, briefing matinal, fechamento).

Design:
  - Stateless: instanciado a cada Task Celery (sem estado entre chamadas).
  - Offline-safe: se o serviço não inicializar (credencial inválida ou API
    desativada), todos os métodos retornam False sem levantar.
  - HTML-only: emails em HTML com CSS inline (Gmail ignora <style> externo).
  - Nunca lança exceção para o caller — todas as falhas são logadas e
    convertidas em False.
"""
from __future__ import annotations

import base64
import logging
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from googleapiclient.discovery import build

from services.google_drive import drive_manager

logger = logging.getLogger("EmailService")

# Identidade do agente de email — criada no Google Workspace da Samba.
SAMBA_AGENT_EMAIL: str = os.getenv(
    "SAMBA_AGENT_EMAIL", "agente@sambaexport.com.br"
)

# ------------------------------------------------------------------
# Template HTML base (CSS inline — compatível com Gmail)
# ------------------------------------------------------------------

_HTML_SHELL = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;padding:0;background:#09090b;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#09090b;">
<tr><td align="center" style="padding:24px 16px;">
  <table width="600" cellpadding="0" cellspacing="0"
         style="background:#141418;border-radius:10px;overflow:hidden;
                border:1px solid rgba(250,130,0,0.18);">

    <!-- HEADER -->
    <tr>
      <td style="background:#09090b;border-top:3px solid #fa8200;
                 padding:20px 32px 16px;">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td>
              <div style="color:#fa8200;font-size:18px;font-weight:700;
                          letter-spacing:3px;text-transform:uppercase;">
                SAMBA EXPORT
              </div>
              <div style="color:#9a9aa0;font-size:10px;letter-spacing:1.5px;
                          text-transform:uppercase;margin-top:2px;">
                Global Commodities Control Desk &mdash; Samba Agent 🤖
              </div>
            </td>
            <td align="right">
              <div style="color:#fa8200;font-size:22px;">{ICON}</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>

    <!-- TITLE BANNER -->
    <tr>
      <td style="padding:20px 32px 4px;border-bottom:1px solid rgba(245,245,247,0.06);">
        <div style="color:#f5f5f7;font-size:17px;font-weight:600;">{TITLE}</div>
        <div style="color:#9a9aa0;font-size:12px;margin-top:4px;">{SUBTITLE}</div>
      </td>
    </tr>

    <!-- BODY -->
    <tr>
      <td style="padding:24px 32px;">{BODY}</td>
    </tr>

    <!-- FOOTER -->
    <tr>
      <td style="padding:14px 32px;border-top:1px solid rgba(245,245,247,0.06);
                 background:#0d0d10;">
        <div style="color:#9a9aa0;font-size:10px;line-height:1.6;">
          Mensagem automática gerada por <strong style="color:#fa8200;">
          {AGENT_EMAIL}</strong>.<br>
          Não responda diretamente — este endereço é de envio apenas.<br>
          <a href="https://sambaexport.com.br" style="color:#fa8200;text-decoration:none;">
          sambaexport.com.br</a>
        </div>
      </td>
    </tr>

  </table>
</td></tr>
</table>
</body>
</html>
"""

# ------------------------------------------------------------------
# Bloco reutilizável: tabela de KPIs / seção
# ------------------------------------------------------------------

def _section(title: str, rows_html: str) -> str:
    """Agrupa linhas dentro de um bloco com título na cor oficial Samba."""
    return (
        f'<div style="margin-bottom:20px;">'
        f'<div style="color:#fa8200;font-size:11px;font-weight:700;'
        f'letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px;">'
        f'{title}</div>'
        f'{rows_html}'
        f'</div>'
    )


def _row(label: str, value: str, highlight: bool = False) -> str:
    color = "#f5f5f7" if highlight else "#c0c0c8"
    return (
        f'<table width="100%" cellpadding="6" cellspacing="0" '
        f'style="background:#1a1a20;border-radius:4px;margin-bottom:4px;">'
        f'<tr>'
        f'<td style="color:#9a9aa0;font-size:12px;">{label}</td>'
        f'<td align="right" style="color:{color};font-size:12px;'
        f'font-weight:{"600" if highlight else "400"};">{value}</td>'
        f'</tr></table>'
    )


def _deal_pill(name: str, commodity: str, stage: str, assignee: str) -> str:
    stage_colors = {
        "Qualificação":   "#fa8200",   # laranja oficial Samba
        "Negociação":     "#326496",   # azul oficial Samba
        "Em Negociação":  "#326496",
        "Fechado":        "#329632",   # verde oficial Samba
        "Perdido":        "#fa3232",   # vermelho oficial Samba
        "Lead Capturado": "#64c8fa",   # azul claro oficial Samba
    }
    sc = stage_colors.get(stage, "#7f8c8d")
    return (
        f'<div style="background:#1a1a20;border-left:3px solid {sc};'
        f'border-radius:4px;padding:8px 12px;margin-bottom:6px;">'
        f'<span style="color:#f5f5f7;font-size:12px;font-weight:600;">{name}</span>'
        f'<span style="color:#9a9aa0;font-size:11px;"> · {commodity}</span>'
        f'<span style="float:right;color:{sc};font-size:10px;'
        f'font-weight:700;text-transform:uppercase;">{stage}</span><br>'
        f'<span style="color:#9a9aa0;font-size:10px;">Resp: {assignee}</span>'
        f'</div>'
    )


# ------------------------------------------------------------------
# Serviço principal
# ------------------------------------------------------------------

class EmailService:
    """
    Cliente fino para envio de emails via Gmail API v1.

    Instanciar uma vez por tarefa Celery — não guarda estado entre chamadas.
    """

    def __init__(self) -> None:
        self.service: Any = None
        self._init_service()

    def _init_service(self) -> None:
        if not drive_manager.creds or not drive_manager.creds.valid:
            logger.warning(
                "EmailService: credenciais DriveManager ausentes/expiradas — "
                "emails não serão enviados."
            )
            return
        try:
            self.service = build("gmail", "v1", credentials=drive_manager.creds)
            logger.info("EmailService: Gmail API v1 inicializada.")
        except Exception as exc:
            logger.error("EmailService: falha ao inicializar Gmail API — %s", exc)

    # ----------------------------------------------------------
    # Primitiva de envio
    # ----------------------------------------------------------

    def send_html(
        self,
        to: str | list[str],
        subject: str,
        html_body: str,
        cc: list[str] | None = None,
    ) -> bool:
        """
        Envia email HTML.

        Args:
            to:       Destinatário(s) — string ou lista.
            subject:  Assunto do email.
            html_body: Corpo HTML completo (use `build_html` para usar o template).
            cc:       Lista de endereços em cópia (opcional).

        Returns:
            True se enviado com sucesso, False caso contrário.
        """
        if not self.service:
            logger.warning("EmailService inativo — email '%s' descartado.", subject)
            return False

        recipients = [to] if isinstance(to, str) else list(to)
        all_to = recipients + (cc or [])

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = SAMBA_AGENT_EMAIL
            msg["To"]      = ", ".join(recipients)
            if cc:
                msg["Cc"] = ", ".join(cc)
            msg.attach(MIMEText(html_body, "html", "utf-8"))

            raw_bytes = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            self.service.users().messages().send(
                userId="me",
                body={"raw": raw_bytes},
            ).execute()
            logger.info(
                "Email enviado: '%s' → %s", subject, all_to
            )
            return True
        except Exception as exc:
            logger.error("Falha ao enviar email '%s': %s", subject, exc)
            return False

    # ----------------------------------------------------------
    # Builder do template
    # ----------------------------------------------------------

    @staticmethod
    def build_html(
        title: str,
        subtitle: str,
        body_html: str,
        icon: str = "📬",
    ) -> str:
        """Monta o HTML completo usando o shell de template."""
        return _HTML_SHELL.format(
            TITLE=title,
            SUBTITLE=subtitle,
            BODY=body_html,
            ICON=icon,
            AGENT_EMAIL=SAMBA_AGENT_EMAIL,
        )

    # ----------------------------------------------------------
    # Helpers de bloco (reexportados para conveniência)
    # ----------------------------------------------------------

    @staticmethod
    def section(title: str, rows_html: str) -> str:
        return _section(title, rows_html)

    @staticmethod
    def row(label: str, value: str, highlight: bool = False) -> str:
        return _row(label, value, highlight)

    @staticmethod
    def deal_pill(name: str, commodity: str, stage: str, assignee: str) -> str:
        return _deal_pill(name, commodity, stage, assignee)
