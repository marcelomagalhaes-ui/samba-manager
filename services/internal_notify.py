"""
services/internal_notify.py
============================
Dispatcher de notificações internas para o time da Samba Export.

Cada alerta é enviado por DOIS canais simultâneos:
  1. Email corporativo → assignee (+ CC ao agente)
  2. WhatsApp          → grupo interno corporativo

O dispatcher NUNCA notifica o contato externo (fornecedor/comprador).
Quem decide abordar o parceiro é o humano da Samba que recebeu o alerta.

Variáveis de ambiente:
  SAMBA_AGENT_EMAIL      — remetente / CC de todos os emails
  INTERNAL_WPP_GROUP     — ID/número do grupo WhatsApp interno corporativo
  EMAIL_LEONARDO         — email do sócio responsável por grãos/farelo
  EMAIL_NIVIO            — email do sócio responsável por açúcar/algodão/etanol
  EMAIL_MARCELO          — email do sócio responsável por proteína/óleo/café
  WHATSAPP_OFFLINE       — se "true", simula envios (sem chamar Twilio)
"""
from __future__ import annotations

import logging
import os
from typing import Any

from services.email_service import EmailService, SAMBA_AGENT_EMAIL
from services.whatsapp_api import get_whatsapp_manager, AgentRole

logger = logging.getLogger("InternalNotify")

# ------------------------------------------------------------------
# Mapa assignee → email corporativo
# ------------------------------------------------------------------

ASSIGNEE_EMAILS: dict[str, str] = {
    "Leonardo": os.getenv("EMAIL_LEONARDO", "lbd@sambaexport.com.br"),
    "Nivio":    os.getenv("EMAIL_NIVIO",    "nmd@sambaexport.com.br"),
    "Marcelo":  os.getenv("EMAIL_MARCELO",  "marcelo.magalhaes@sambaexport.com.br"),
}

# Grupo WhatsApp interno (número E.164 ou SID Twilio do grupo).
INTERNAL_WPP_GROUP: str = os.getenv("INTERNAL_WPP_GROUP", "")


# ------------------------------------------------------------------
# Dispatcher principal
# ------------------------------------------------------------------

class InternalNotifyService:
    """
    Envia alertas simultâneos por email + WhatsApp ao time interno.
    Instanciar por chamada (stateless).
    """

    def __init__(self) -> None:
        self.email = EmailService()
        self.wpp   = get_whatsapp_manager()

    # ----------------------------------------------------------
    # Envio WhatsApp interno
    # ----------------------------------------------------------

    def _send_wpp(self, text: str) -> bool:
        """Envia mensagem ao grupo interno via agente Manager."""
        if not INTERNAL_WPP_GROUP:
            logger.warning(
                "InternalNotify: INTERNAL_WPP_GROUP não configurado — WhatsApp ignorado."
            )
            return False
        try:
            self.wpp.send(AgentRole.MANAGER, INTERNAL_WPP_GROUP, text)
            return True
        except Exception as exc:
            logger.error("InternalNotify: falha WhatsApp — %s", exc)
            return False

    def send_internal_wpp(self, text: str) -> bool:
        """
        Envia mensagem livre ao grupo WhatsApp interno corporativo.

        Método público para uso pelas Sprint M tasks (Morning Pulse, Drive Status,
        Geopolitical Sentinel, Voice ATA). Delega para `_send_wpp` internamente.

        Returns:
            True  se enviado com sucesso (ou simulado em WHATSAPP_OFFLINE).
            False se INTERNAL_WPP_GROUP não configurado ou erro de envio.
        """
        logger.info("send_internal_wpp: enviando %d chars ao grupo interno", len(text))
        return self._send_wpp(text)

    # ----------------------------------------------------------
    # Alerta: deal com campos faltantes
    # ----------------------------------------------------------

    def alert_missing_fields(
        self,
        deal_id: int,
        deal_name: str,
        commodity: str,
        assignee: str,
        source_sender: str,
        source_group: str,
        missing: list[str],
        original_text: str = "",
    ) -> dict[str, bool]:
        """
        Notifica o assignee por email + WhatsApp que um deal entrou em
        Qualificação por falta de dados e precisa de ação humana.
        """
        assignee_email = ASSIGNEE_EMAILS.get(assignee, SAMBA_AGENT_EMAIL)

        _label_map = {
            "Produto/Especificação":                "Produto / Especificação exata",
            "Volume (MT/Sacas)":                    "Volume total (MT ou sacas)",
            "Incoterm (FOB/CIF/etc)":               "Incoterm desejado (FOB, CIF, CFR…)",
            "Target Price":                         "Preço-alvo (USD/MT)",
            "Porto de Destino (Necessário para CIF)": "Porto de destino (CIF/CFR)",
        }
        items_html = "".join(
            f'<li style="color:#f5f5f7;font-size:13px;margin-bottom:4px;">'
            f'{_label_map.get(m, m)}</li>'
            for m in missing
        )
        body_html = (
            self.email.section(
                "Deal em Qualificação",
                self.email.deal_pill(deal_name, commodity, "Qualificação", assignee),
            )
            + self.email.section(
                "Remetente / Grupo",
                self.email.row("Contato", source_sender or "—")
                + self.email.row("Grupo WhatsApp", source_group or "—"),
            )
            + self.email.section(
                "Informações faltantes (cobrar do parceiro)",
                f'<ul style="margin:0;padding-left:18px;">{items_html}</ul>',
            )
            + (
                f'<div style="margin-top:16px;padding:12px 16px;background:#1a1a20;'
                f'border-radius:6px;border-left:3px solid #fa8200;">'
                f'<span style="color:#fa8200;font-size:12px;font-weight:700;">⚡ AÇÃO NECESSÁRIA</span>'
                f'<p style="color:#c0c0c8;font-size:12px;margin:6px 0 0;">'
                f'Por favor, entre em contato com <strong style="color:#f5f5f7;">{source_sender or source_group}</strong>'
                f' para obter os dados acima e atualizar o deal no painel.</p>'
                f'</div>'
            )
        )

        # Bloco com texto original da mensagem WhatsApp (se disponível)
        orig_block = ""
        if original_text:
            safe = original_text.replace("<", "&lt;").replace(">", "&gt;")[:500]
            orig_block = (
                f'<div style="margin-top:16px;">'
                f'<div style="color:#fa8200;font-size:11px;font-weight:700;'
                f'letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px;">'
                f'Mensagem Original (WhatsApp)</div>'
                f'<div style="background:#0d0d10;border-left:3px solid #9a9aa0;'
                f'border-radius:4px;padding:10px 14px;">'
                f'<span style="color:#c0c0c8;font-size:12px;font-family:monospace;'
                f'white-space:pre-wrap;line-height:1.6;">{safe}</span>'
                f'</div></div>'
            )

        html = self.email.build_html(
            title="⚠️ Deal incompleto — ação necessária",
            subtitle=f"Deal #{deal_id} · {deal_name} · Entrada: Qualificação",
            body_html=body_html + orig_block,
            icon="⚠️",
        )

        todos = list(ASSIGNEE_EMAILS.values())   # todos os sócios sempre

        email_ok = self.email.send_html(
            to=todos,
            subject=f"[Samba] Deal incompleto: {deal_name} — {', '.join(missing[:2])} faltando",
            html_body=html,
            cc=[SAMBA_AGENT_EMAIL],
        )

        wpp_text = (
            f"⚠️ *DEAL INCOMPLETO — AÇÃO NECESSÁRIA*\n\n"
            f"*Deal:* {deal_name}\n"
            f"*Commodity:* {commodity}\n"
            f"*Contato:* {source_sender or source_group or '?'}\n"
            f"*Resp:* {assignee}\n\n"
            f"*Falta cobrar:*\n"
            + "\n".join(f"  • {_label_map.get(m, m)}" for m in missing)
            + f"\n\n👉 Acesse o painel e atualize o deal após obter as informações."
        )
        wpp_ok = self._send_wpp(wpp_text)

        return {"email": email_ok, "whatsapp": wpp_ok}

    # ----------------------------------------------------------
    # Despacho de follow-up via email (WhatsApp offline)
    # ----------------------------------------------------------

    def alert_followup_dispatch(
        self,
        followup_id: int,
        deal_name: str,
        commodity: str,
        assignee: str,
        partner: str,
        message_text: str,
        days_overdue: int = 0,
        deal_snapshot: dict | None = None,
    ) -> dict[str, bool]:
        """
        Enviado quando WHATSAPP_OFFLINE=true.

        Email completo ao assignee com:
          - Snapshot do deal (o que sabemos + o que falta)
          - Mensagem original do WhatsApp do parceiro
          - Mensagem gerada pela IA pronta para copiar e enviar
        """
        assignee_email = ASSIGNEE_EMAILS.get(assignee, SAMBA_AGENT_EMAIL)
        snap = deal_snapshot or {}

        urgency_label = (
            "SUAVE"   if days_overdue <= 2 else
            "MÉDIO"   if days_overdue <= 5 else
            "URGENTE"
        )
        urgency_color = (
            "#fa8200" if days_overdue <= 2 else
            "#fa8200" if days_overdue <= 5 else
            "#fa3232"   # vermelho oficial Samba para urgente
        )

        # ── Seção 1: pill do deal ─────────────────────────────────
        section_deal = self.email.section(
            "Deal em Qualificação",
            self.email.deal_pill(deal_name, commodity, "Qualificação", assignee),
        )

        # ── Seção 2: origem / parceiro ────────────────────────────
        section_origem = self.email.section(
            "Origem",
            self.email.row("Remetente",   snap.get("remetente") or partner or "—")
            + self.email.row("Grupo WPP",  snap.get("grupo_wpp") or "—")
            + self.email.row("Capturado em", snap.get("criado_em") or "—")
            + self.email.row("Aguardando", f"{days_overdue} dia(s)",
                             highlight=(days_overdue >= 3)),
        )

        # ── Seção 3: o que sabemos do deal ───────────────────────
        def _val(v): return str(v) if v else "—"
        dir_color = {
            "BID": "#329632", "ASK": "#326496", "UNKNOWN": "#9a9aa0"
        }.get(snap.get("direcao", "UNKNOWN"), "#9a9aa0")

        direcao_html = (
            f'<span style="background:rgba(0,0,0,0.3);color:{dir_color};'
            f'font-weight:700;padding:2px 8px;border-radius:10px;font-size:11px;">'
            f'{snap.get("direcao","UNKNOWN")}</span>'
        )

        section_dados = self.email.section(
            "Dados do Negócio (capturados)",
            self.email.row("Direção",  direcao_html)
            + self.email.row("Volume",   _val(snap.get("volume")))
            + self.email.row("Preço",    _val(snap.get("preco")))
            + self.email.row("Incoterm", _val(snap.get("incoterm")))
            + self.email.row("Origem",   _val(snap.get("origem")))
            + self.email.row("Destino",  _val(snap.get("destino"))),
        )

        # ── Seção 4: campos faltantes (extraídos do EXTRACTOR_WARN) ─
        notes = snap.get("notes", "")
        missing_fields: list[str] = []
        if notes and "EXTRACTOR_WARN" in notes:
            for line in notes.splitlines():
                if line.startswith("Campos faltantes detectados:"):
                    raw = line.replace("Campos faltantes detectados:", "").strip()
                    missing_fields = [f.strip() for f in raw.split(",") if f.strip()]
                    break

        section_faltantes = ""
        if missing_fields:
            items_html = "".join(
                f'<li style="color:#f5f5f7;font-size:13px;margin-bottom:4px;">{m}</li>'
                for m in missing_fields
            )
            section_faltantes = self.email.section(
                "⚠️ Campos Faltantes (cobrar do parceiro)",
                f'<ul style="margin:0;padding-left:18px;">{items_html}</ul>',
            )

        # ── Seção 5: mensagem original do WhatsApp ────────────────
        orig_wpp = ""
        if notes and "[WHATSAPP] Texto Original:" in notes:
            try:
                start = notes.index("[WHATSAPP] Texto Original:") + len("[WHATSAPP] Texto Original:")
                end   = notes.find("[", start)
                raw_orig = (notes[start:end] if end > start else notes[start:]).strip()
                if raw_orig:
                    safe_orig = raw_orig.replace("<", "&lt;").replace(">", "&gt;")[:600]
                    orig_wpp = (
                        f'<div style="margin-top:14px;background:#0d0d10;'
                        f'border-left:3px solid #9a9aa0;border-radius:4px;padding:12px 16px;">'
                        f'<div style="color:#9a9aa0;font-size:11px;font-weight:700;'
                        f'letter-spacing:1.5px;text-transform:uppercase;margin-bottom:8px;">'
                        f'Mensagem Original (WhatsApp)</div>'
                        f'<span style="color:#c0c0c8;font-size:12px;font-family:monospace;'
                        f'white-space:pre-wrap;line-height:1.6;">{safe_orig}</span>'
                        f'</div>'
                    )
            except (ValueError, IndexError):
                pass

        # ── Seção 6: CTA + mensagem gerada ───────────────────────
        safe_msg = message_text.replace("<", "&lt;").replace(">", "&gt;")
        section_acao = (
            f'<div style="margin-top:16px;padding:12px 16px;background:#0d1a00;'
            f'border-radius:6px;border-left:3px solid {urgency_color};">'
            f'<span style="color:{urgency_color};font-size:12px;font-weight:700;">'
            f'📋 AÇÃO [{urgency_label}]: copie a mensagem abaixo e envie ao parceiro via WhatsApp</span>'
            f'<p style="color:#c0c0c8;font-size:12px;margin:6px 0 0;">'
            f'WhatsApp do agente ainda não está ativo. Envio manual até ativação do Twilio.</p>'
            f'</div>'
            f'<div style="margin-top:10px;background:#0d0d10;border-left:3px solid #fa8200;'
            f'border-radius:4px;padding:12px 16px;">'
            f'<div style="color:#fa8200;font-size:11px;font-weight:700;letter-spacing:1.5px;'
            f'text-transform:uppercase;margin-bottom:8px;">Mensagem gerada pela IA — copiar e enviar</div>'
            f'<span style="color:#f5f5f7;font-size:13px;font-family:monospace;'
            f'white-space:pre-wrap;line-height:1.7;">{safe_msg}</span>'
            f'</div>'
        )

        body_html = (
            section_deal
            + section_origem
            + section_dados
            + section_faltantes
            + (self.email.section("Contexto da Conversa", orig_wpp) if orig_wpp else "")
            + section_acao
        )

        html = self.email.build_html(
            title=f"📋 Follow-up [{urgency_label}]",
            subtitle=f"Deal #{snap.get('id','?')} · {deal_name} · {days_overdue}d aguardando resposta",
            body_html=body_html,
            icon="📋",
        )

        todos = list(ASSIGNEE_EMAILS.values())   # todos os sócios sempre

        email_ok = self.email.send_html(
            to=todos,
            subject=f"[Samba] Follow-up {urgency_label}: {deal_name} — {commodity} | {days_overdue}d",
            html_body=html,
            cc=[SAMBA_AGENT_EMAIL],
        )

        logger.info(
            "alert_followup_dispatch: followup_id=%s deal=%s email_ok=%s destinatarios=%s",
            followup_id, deal_name, email_ok, todos,
        )
        return {"email": email_ok, "whatsapp": False}

    # ----------------------------------------------------------
    # Alerta: follow-up respondido
    # ----------------------------------------------------------

    def alert_followup_responded(
        self,
        followup_id: int,
        deal_id: int | None,
        deal_name: str,
        commodity: str,
        assignee: str,
        target_person: str,
        response_content: str,
    ) -> dict[str, bool]:
        """
        Notifica o assignee que um parceiro respondeu ao follow-up enviado.
        Enviado por email + WhatsApp ao grupo interno.
        """
        assignee_email = ASSIGNEE_EMAILS.get(assignee, SAMBA_AGENT_EMAIL)

        safe_response = response_content.replace("<", "&lt;").replace(">", "&gt;")[:800]
        response_block = (
            f'<div style="margin-top:14px;background:#0d0d10;border-left:3px solid #329632;'
            f'border-radius:4px;padding:10px 14px;">'
            f'<div style="color:#329632;font-size:11px;font-weight:700;letter-spacing:1.5px;'
            f'text-transform:uppercase;margin-bottom:6px;">Mensagem Recebida (WhatsApp)</div>'
            f'<span style="color:#c0c0c8;font-size:12px;font-family:monospace;'
            f'white-space:pre-wrap;line-height:1.6;">{safe_response or "—"}</span>'
            f'</div>'
        ) if safe_response else ""

        body_html = (
            self.email.section(
                "Deal com Resposta",
                self.email.deal_pill(deal_name, commodity, "Em Negociação", assignee),
            )
            + self.email.section(
                "Parceiro",
                self.email.row("Contato", target_person or "—")
                + self.email.row("Follow-Up #", str(followup_id)),
            )
            + (
                f'<div style="margin-top:16px;padding:12px 16px;background:#001a12;'
                f'border-radius:6px;border-left:3px solid #329632;">'
                f'<span style="color:#329632;font-size:12px;font-weight:700;">✅ PARCEIRO RESPONDEU</span>'
                f'<p style="color:#c0c0c8;font-size:12px;margin:6px 0 0;">'
                f'O deal pode avançar para <strong style="color:#f5f5f7;">Em Negociação</strong>. '
                f'Acesse o painel para dar continuidade.</p>'
                f'</div>'
            )
            + response_block
        )

        html = self.email.build_html(
            title="✅ Follow-up Respondido",
            subtitle=f"Deal #{deal_id} · {deal_name} · Parceiro retornou contato",
            body_html=body_html,
            icon="✅",
        )

        todos = list(ASSIGNEE_EMAILS.values())   # todos os sócios sempre

        email_ok = self.email.send_html(
            to=todos,
            subject=f"[Samba] Resposta recebida: {deal_name} — {target_person}",
            html_body=html,
            cc=[SAMBA_AGENT_EMAIL],
        )

        wpp_text = (
            f"✅ *FOLLOW-UP RESPONDIDO*\n\n"
            f"*Deal:* {deal_name}\n"
            f"*Commodity:* {commodity}\n"
            f"*Parceiro:* {target_person or '?'}\n"
            f"*Resp:* {assignee}\n\n"
            f"*Mensagem:*\n{response_content[:300] or '—'}"
            f"\n\n👉 Acesse o painel para avançar o deal."
        )
        wpp_ok = self._send_wpp(wpp_text)

        return {"email": email_ok, "whatsapp": wpp_ok}

    # ----------------------------------------------------------
    # Briefing matinal
    # ----------------------------------------------------------

    def send_morning_brief(self, stats: dict[str, Any]) -> bool:
        """
        Envia email de briefing matinal para todos os sócios.

        stats esperado:
          deals_ontem   : int
          qualificacoes : list[dict]  — deals em Qualificação
          followups_vencidos: int
          fechados_ontem: int
          pipeline_por_stage: dict[str, int]
        """
        destinatarios = list(ASSIGNEE_EMAILS.values())

        body = ""

        # KPIs do pipeline
        por_stage = stats.get("pipeline_por_stage", {})
        kpi_rows = "".join(
            self.email.row(stage, str(count), highlight=(stage == "Qualificação"))
            for stage, count in por_stage.items()
        )
        body += self.email.section("Pipeline Atual", kpi_rows or "<p style='color:#9a9aa0;font-size:12px;'>Sem dados.</p>")

        # Deals criados ontem
        body += self.email.section(
            "Deals Criados Ontem",
            self.email.row("Total capturado", str(stats.get("deals_ontem", 0)), highlight=True)
            + self.email.row("Fechados ontem", str(stats.get("fechados_ontem", 0))),
        )

        # Qualificações abertas
        quals = stats.get("qualificacoes", [])
        if quals:
            pills = "".join(
                self.email.deal_pill(
                    q.get("name", "?"), q.get("commodity", "?"),
                    "Qualificação", q.get("assignee", "?")
                )
                for q in quals[:10]  # max 10 no email
            )
            body += self.email.section(
                f"Qualificações Abertas ({len(quals)})",
                pills,
            )

        # Follow-ups vencidos
        fv = stats.get("followups_vencidos", 0)
        if fv:
            body += self.email.section(
                "Follow-ups Vencidos",
                self.email.row("Aguardando despacho", str(fv), highlight=True),
            )

        html = self.email.build_html(
            title="🌅 Briefing Matinal — Samba Export",
            subtitle=f"Resumo do dia anterior e situação atual do pipeline",
            body_html=body,
            icon="🌅",
        )

        return self.email.send_html(
            to=destinatarios,
            subject="[Samba] ☀️ Briefing Matinal — Painel Comercial",
            html_body=html,
        )

    # ----------------------------------------------------------
    # Fechamento do dia
    # ----------------------------------------------------------

    def send_eod_closing(self, stats: dict[str, Any]) -> bool:
        """
        Envia email de fechamento diário para todos os sócios.

        stats esperado:
          deals_hoje       : int
          avancados_hoje   : list[dict]  — deals que mudaram de stage hoje
          qualificacoes_abertas: int
          fechados_hoje    : int
          perdidos_hoje    : int
        """
        destinatarios = list(ASSIGNEE_EMAILS.values())

        body = self.email.section(
            "Atividade de Hoje",
            self.email.row("Deals capturados", str(stats.get("deals_hoje", 0)))
            + self.email.row("Fechamentos", str(stats.get("fechados_hoje", 0)), highlight=True)
            + self.email.row("Perdidos", str(stats.get("perdidos_hoje", 0)))
            + self.email.row("Em aberto (Qualificação)", str(stats.get("qualificacoes_abertas", 0))),
        )

        avancados = stats.get("avancados_hoje", [])
        if avancados:
            pills = "".join(
                self.email.deal_pill(
                    a.get("name", "?"), a.get("commodity", "?"),
                    a.get("stage", "?"), a.get("assignee", "?")
                )
                for a in avancados[:10]
            )
            body += self.email.section(f"Deals que Avançaram Hoje ({len(avancados)})", pills)

        html = self.email.build_html(
            title="🌙 Fechamento do Dia — Samba Export",
            subtitle="Resumo das atividades de hoje",
            body_html=body,
            icon="🌙",
        )

        return self.email.send_html(
            to=destinatarios,
            subject="[Samba] 🌙 Fechamento do Dia — Painel Comercial",
            html_body=html,
        )

    # ----------------------------------------------------------
    # Alerta intraday
    # ----------------------------------------------------------

    def send_intraday_alert(self, stale_deals: list[dict[str, Any]]) -> bool:
        """
        Dispara somente quando há deals em Qualificação há >2h sem follow-up
        pendente — evita spam quando o pipeline está limpo.
        """
        if not stale_deals:
            logger.info("InternalNotify intraday: nenhuma pendência — alerta suprimido.")
            return True  # nada a fazer é sucesso

        destinatarios = list(ASSIGNEE_EMAILS.values())

        pills = "".join(
            self.email.deal_pill(
                d.get("name", "?"), d.get("commodity", "?"),
                "Qualificação", d.get("assignee", "?")
            )
            for d in stale_deals[:15]
        )
        body = self.email.section(
            f"{len(stale_deals)} deal(s) em Qualificação sem follow-up (>2h)",
            pills
            + (
                f'<div style="margin-top:12px;padding:10px 14px;background:#1a0a00;'
                f'border-left:3px solid #fa8200;border-radius:4px;">'
                f'<span style="color:#fa8200;font-size:12px;">Ação: acesse o painel, '
                f'obtenha os dados faltantes com os parceiros e atualize os deals.</span>'
                f'</div>'
            ),
        )

        html = self.email.build_html(
            title="⏰ Atenção: Deals parados em Qualificação",
            subtitle=f"{len(stale_deals)} deal(s) aguardam informações há mais de 2 horas",
            body_html=body,
            icon="⏰",
        )

        email_ok = self.email.send_html(
            to=destinatarios,
            subject=f"[Samba] ⏰ {len(stale_deals)} deal(s) parado(s) — ação necessária",
            html_body=html,
        )

        wpp_text = (
            f"⏰ *{len(stale_deals)} deal(s) em Qualificação sem resposta há >2h*\n\n"
            + "\n".join(
                f"  • {d.get('name','?')} | {d.get('commodity','?')} | Resp: {d.get('assignee','?')}"
                for d in stale_deals[:8]
            )
            + "\n\n👉 Acesse o painel e dê seguimento."
        )
        self._send_wpp(wpp_text)

        return email_ok


# ------------------------------------------------------------------
# Instância singleton leve (recriada se creds expirarem)
# ------------------------------------------------------------------

def get_notifier() -> InternalNotifyService:
    """Factory — cria nova instância por chamada (creds podem ter rotacionado)."""
    return InternalNotifyService()
