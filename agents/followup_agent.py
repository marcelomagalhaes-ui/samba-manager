"""
agents/followup_agent.py
========================
O COBRADOR da Samba Export — Agente Follow-Up.

Responsabilidades:
  (a) Lê a tabela FollowUp do banco buscando prazos vencidos
  (b) Gera mensagem de cobrança personalizada via Claude
  (c) Envia via WhatsAppManager role=FOLLOWUP
  (d) Registra sent_at e atualiza status no banco
  (e) Monitora contra-respostas: detecta FollowUps respondidos e avança o Deal

Uso:
    python agents/followup_agent.py
    python agents/followup_agent.py --check-responses   # só verifica respostas pendentes
    python agents/followup_agent.py --dry-run           # não envia mensagens
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os

from agents.base_agent import BaseAgent
from models.database import get_session, Deal, FollowUp
from services.gemini_api import ask_gemini as ask_claude, MODEL_FAST
from services.whatsapp_api import get_whatsapp_manager, AgentRole

# Se True, despacha via email ao assignee em vez de WhatsApp ao parceiro externo.
# Remove quando o número Twilio estiver ativo.
WHATSAPP_OFFLINE = os.getenv("WHATSAPP_OFFLINE", "true").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


# Depois de quantos dias sem resposta escalar para o Manager
ESCALATE_AFTER_DAYS = 3

# System prompt do Follow-Up — compacto e focado em cobrança
_FOLLOWUP_SYSTEM = """Você é o Agente Follow-Up da Samba Export, especializado em recuperar negócios parados.

Sua voz: direta, amigável, sem pressão excessiva. Tom de parceiro de mercado, não de cobrador agressivo.
Idioma: português brasileiro natural, como se conversa no WhatsApp entre operadores de commodities.

Regras absolutas:
- Máximo 4 frases por mensagem
- NUNCA comece com "Espero que esteja bem"
- NUNCA use linguagem jurídica ou ameaças
- Sempre inclua 1 call-to-action claro
- Use no máximo 1 emoji por mensagem"""


class FollowUpAgent(BaseAgent):
    """
    Agente Follow-Up — cobra respostas, mantém negociações vivas,
    monitora contra-propostas e escalona para o Manager quando necessário.
    """

    name = "FollowUpAgent"
    description = (
        "Cobrador inteligente: monitora prazos, envia cobranças personalizadas "
        "e detecta quando um deal precisa ser escalado."
    )
    visible_in_groups = True    # Entra em grupos e responde ativamente
    generates_spreadsheets = False

    def __init__(self):
        super().__init__()
        self.session = get_session()
        self.wpp = get_whatsapp_manager()

    # ──────────────────────────────────────────────────────────
    # process() — entry point do BaseAgent.run()
    # ──────────────────────────────────────────────────────────

    def process(self, data: Any = None) -> dict:
        """
        Ciclo completo do Follow-Up:
          1. Busca follow-ups vencidos (due_at <= agora, status='pendente')
          2. Para cada um: gera mensagem → envia → registra
          3. Verifica follow-ups enviados com respostas
          4. Escalona casos sem resposta há muito tempo

        Args:
            data: dict opcional com:
                - dry_run: bool (não envia WA, default False)
                - check_responses: bool (só verifica respostas, default False)
                - max_batch: int (limite de envios por ciclo, default 20)
        """
        if data is None:
            data = {}

        dry_run = data.get("dry_run", False)
        check_responses = data.get("check_responses", False)
        max_batch = data.get("max_batch", 20)

        self.log_action("cycle_started", {
            "dry_run": dry_run,
            "check_responses": check_responses,
        })

        results: dict[str, Any] = {
            "sent": 0,
            "simulated": 0,   # dry_run=True: contados aqui, não em "sent"
            "skipped": 0,
            "errors": 0,
            "responses_processed": 0,
            "escalated": 0,
        }

        # Fase 1: verificar respostas recebidas (sempre, independente do modo)
        resp_count = self._process_responses()
        results["responses_processed"] = resp_count

        if check_responses:
            return results  # modo só-verificação

        # Fase 2: enviar cobranças para follow-ups vencidos
        overdue = self._get_overdue_followups(max_batch)
        self.log_action("overdue_found", {"count": len(overdue)})

        for fu in overdue:
            try:
                outcome = self._process_single_followup(fu, dry_run=dry_run)
                if outcome == "sent":
                    results["sent"] += 1
                elif outcome == "simulated":
                    results["simulated"] += 1
                else:
                    results["skipped"] += 1
            except Exception as exc:
                logger.error("Erro ao processar follow-up %d: %s", fu.id, exc)
                self.log_action("followup_error", {"id": fu.id, "error": str(exc)}, level="ERROR")
                results["errors"] += 1

        # Fase 3: escalonar casos sem resposta há muito tempo
        escalated = self._escalate_stale_followups(dry_run=dry_run)
        results["escalated"] = escalated

        self.session.commit()
        self.log_action("cycle_finished", results)
        return results

    # ──────────────────────────────────────────────────────────
    # (a) Buscar follow-ups vencidos
    # ──────────────────────────────────────────────────────────

    def _get_overdue_followups(self, limit: int = 20) -> list[FollowUp]:
        """
        Retorna follow-ups pendentes com prazo vencido, ordenados por urgência.

        Critérios:
          - status = "pendente"
          - due_at <= agora
          - sent_at IS NULL (ainda não enviado)
        """
        now = datetime.utcnow()
        return (
            self.session.query(FollowUp)
            .filter(
                FollowUp.status == "pendente",
                FollowUp.due_at <= now,
                FollowUp.sent_at.is_(None),
            )
            .order_by(FollowUp.due_at.asc())  # mais antigo primeiro
            .limit(limit)
            .all()
        )

    # ──────────────────────────────────────────────────────────
    # (b) Processar um follow-up individual
    # ──────────────────────────────────────────────────────────

    def _process_single_followup(self, fu: FollowUp, dry_run: bool = False) -> str:
        """
        Para um FollowUp específico:
          1. Recupera o Deal associado para contexto
          2. Gera mensagem personalizada via Gemini
          3. Despacha pelo canal disponível:
               - dry_run=True      → só loga, não altera banco  → "simulated"
               - WHATSAPP_OFFLINE  → email ao assignee com o texto pronto → "sent"
               - WhatsApp ativo    → wpp.send() ao parceiro externo       → "sent"
          4. Atualiza banco (só quando não dry_run)

        Retorna: "sent" | "simulated" | "skipped"
        """
        deal = None
        if fu.deal_id:
            deal = self.session.query(Deal).filter(Deal.id == fu.deal_id).first()

        context = self._build_context(fu, deal)
        message = self._generate_followup_message(fu, context)

        days_overdue = (datetime.utcnow() - fu.due_at).days
        self.log_action("followup_processing", {
            "id": fu.id,
            "target": fu.target_person,
            "group": fu.target_group,
            "days_overdue": days_overdue,
            "channel": "email" if WHATSAPP_OFFLINE else "whatsapp",
        })

        # ── DRY RUN ────────────────────────────────────────────────
        if dry_run:
            logger.info(
                "[DRY RUN] Follow-up #%d | Deal: %s | Para: %s\n%s",
                fu.id,
                deal.name if deal else "—",
                fu.target_person or fu.target_group or "—",
                message,
            )
            return "simulated"   # banco inalterado

        # ── WHATSAPP OFFLINE → email ao assignee ──────────────────
        if WHATSAPP_OFFLINE:
            assignee  = deal.assignee  if deal else (fu.target_person or "Leonardo")
            deal_name = deal.name      if deal else "—"
            commodity = deal.commodity if deal else "—"
            partner   = fu.target_person or fu.target_group or "parceiro"

            # Snapshot completo do deal para o email
            deal_snapshot = {}
            if deal:
                deal_snapshot = {
                    "id":          deal.id,
                    "direcao":     deal.direcao     or "UNKNOWN",
                    "volume":      f"{deal.volume} {deal.volume_unit}" if deal.volume else None,
                    "preco":       f"{deal.currency} {deal.price:,.2f}/MT" if deal.price else None,
                    "incoterm":    deal.incoterm    or None,
                    "origem":      deal.origin      or None,
                    "destino":     deal.destination or None,
                    "risco":       deal.risk_score,
                    "criado_em":   deal.created_at.strftime("%d/%m/%Y %H:%M") if deal.created_at else None,
                    "grupo_wpp":   deal.source_group  or None,
                    "remetente":   deal.source_sender or partner,
                    "notes":       deal.notes or "",
                }

            try:
                from services.internal_notify import get_notifier
                get_notifier().alert_followup_dispatch(
                    followup_id=fu.id,
                    deal_name=deal_name,
                    commodity=commodity,
                    assignee=assignee,
                    partner=partner,
                    message_text=message,
                    days_overdue=days_overdue,
                    deal_snapshot=deal_snapshot,
                )
                self.log_action("followup_email_dispatched", {
                    "id": fu.id, "assignee": assignee, "deal": deal_name,
                })
            except Exception as exc:
                logger.warning("Falha ao enviar email de follow-up #%d: %s", fu.id, exc)
                # Não levanta: regista no banco de qualquer forma para não reprocessar

            fu.sent_at = datetime.utcnow()
            fu.status  = "enviado"
            fu.message = message
            return "sent"

        # ── WHATSAPP ATIVO → envia direto ao parceiro ─────────────
        recipient = fu.target_group or fu.target_person
        if not recipient:
            logger.warning("FollowUp #%d sem destinatário — pulando.", fu.id)
            fu.status = "expirado"
            return "skipped"

        try:
            result = self.wpp.send(AgentRole.FOLLOWUP, recipient, message)
            fu.sent_at = datetime.utcnow()
            fu.status  = "enviado"
            fu.message = message
            self.log_action("followup_wpp_sent", {
                "id": fu.id, "recipient": recipient,
                "sid": result.get("sid"), "preview": message[:100],
            })
            return "sent"
        except Exception as exc:
            logger.error("Falha WA follow-up #%d para %s: %s", fu.id, recipient, exc)
            raise

    def _build_context(self, fu: FollowUp, deal: Deal | None) -> dict:
        """Monta o dict de contexto para a geração da mensagem."""
        days_overdue = max(0, (datetime.utcnow() - fu.due_at).days)

        ctx: dict = {
            "contact_name": fu.target_person or "parceiro",
            "days_since_contact": days_overdue,
            "original_followup_message": fu.message or "",
        }

        if deal:
            ctx.update({
                "commodity": deal.commodity,
                "volume": f"{deal.volume} {deal.volume_unit}" if deal.volume else None,
                "price": f"{deal.currency} {deal.price:,.2f}" if deal.price else None,
                "incoterm": deal.incoterm,
                "deal_stage": deal.stage,
                "deal_name": deal.name,
                "assignee": deal.assignee,
            })

        return ctx

    # ──────────────────────────────────────────────────────────
    # (c) Gerar mensagem de cobrança via Claude
    # ──────────────────────────────────────────────────────────

    def _generate_followup_message(self, fu: FollowUp, context: dict) -> str:
        """
        Gera mensagem de cobrança personalizada via Claude.

        Adapta o tom conforme urgência:
          - 1-2 dias vencido: suave, "só checando"
          - 3-5 dias: mais direto, referencia o negócio
          - 6+ dias: urgente, sinaliza que a janela pode fechar
        """
        days = context.get("days_since_contact", 0)

        if days <= 2:
            urgency = "suave"
            urgency_guide = "Tom casual, apenas relembrando. Sem mencionar prazo."
        elif days <= 5:
            urgency = "médio"
            urgency_guide = "Tom direto. Mencione o negócio específico. Pergunte se ainda há interesse."
        else:
            urgency = "urgente"
            urgency_guide = (
                "Tom mais firme. Sinalize que a janela de preço pode fechar. "
                "Peça confirmação ou sinalização até hoje."
            )

        prompt = f"""Crie uma mensagem de follow-up para WhatsApp de trading de commodities.

Urgência: {urgency} ({days} dias sem resposta)
Diretriz de tom: {urgency_guide}

Contexto do negócio:
{json.dumps(context, ensure_ascii=False, indent=2)}

Mensagem anterior (se houver):
{fu.message or 'Não há mensagem anterior registrada.'}

Crie a mensagem de cobrança agora. Retorne APENAS o texto da mensagem."""

        return ask_claude(
            prompt,
            system=_FOLLOWUP_SYSTEM,
            model=MODEL_FAST,
            max_tokens=256,
            use_rag=False,  # follow-up não precisa de contexto corporativo
        )

    # ──────────────────────────────────────────────────────────
    # (d) Registrar envio — já feito em _process_single_followup
    # ──────────────────────────────────────────────────────────

    # ──────────────────────────────────────────────────────────
    # (e) Monitorar contra-respostas
    # ──────────────────────────────────────────────────────────

    def _process_responses(self) -> int:
        """
        Verifica follow-ups com status='enviado' e response_received=True.
        Quando há resposta:
          - Marca o FollowUp como "respondido"
          - Avança o Deal para "Em Negociação" se ainda estava em "Lead Capturado"
          - Registra log da resposta

        Em produção, response_received é setado pelo webhook Twilio
        (inbound messages) que atualiza o banco via endpoint HTTP.
        Aqui processamos o que já está flagado como True no banco.

        Retorna o número de respostas processadas.
        """
        responded = (
            self.session.query(FollowUp)
            .filter(
                FollowUp.status == "enviado",
                FollowUp.response_received.is_(True),
            )
            .all()
        )

        processed = 0
        for fu in responded:
            fu.status = "respondido"

            # Avançar Deal no pipeline se aplicável
            if fu.deal_id:
                deal = self.session.query(Deal).filter(Deal.id == fu.deal_id).first()
                if deal and deal.stage == "Lead Capturado":
                    deal.stage = "Em Negociação"
                    deal.updated_at = datetime.utcnow()
                    self.log_action("deal_advanced", {
                        "deal_id": deal.id,
                        "deal_name": deal.name,
                        "new_stage": "Em Negociação",
                        "triggered_by": f"FollowUp#{fu.id}",
                    })

            self.log_action("response_processed", {
                "followup_id": fu.id,
                "target": fu.target_person,
                "response_preview": (fu.response_content or "")[:100],
            })
            processed += 1

        return processed

    # ──────────────────────────────────────────────────────────
    # Escalonamento para o Manager
    # ──────────────────────────────────────────────────────────

    def _escalate_stale_followups(self, dry_run: bool = False) -> int:
        """
        Follow-ups enviados há mais de ESCALATE_AFTER_DAYS sem resposta
        são escalados para o Manager (notificação ao sócio responsável).

        Marca o follow-up como 'expirado' para evitar reprocessamento.
        Retorna o número de itens escalados.
        """
        cutoff = datetime.utcnow() - timedelta(days=ESCALATE_AFTER_DAYS)
        stale = (
            self.session.query(FollowUp)
            .filter(
                FollowUp.status == "enviado",
                FollowUp.sent_at <= cutoff,
                FollowUp.response_received.is_(False),
            )
            .all()
        )

        if not stale:
            return 0

        import os
        socios_phones = {
            os.getenv("SOCIO_1_NAME", "Leonardo"): os.getenv("SOCIO_1_PHONE", ""),
            os.getenv("SOCIO_2_NAME", "Nivio"):    os.getenv("SOCIO_2_PHONE", ""),
            os.getenv("SOCIO_3_NAME", "Marcelo"):  os.getenv("SOCIO_3_PHONE", ""),
        }

        escalated_count = 0
        for fu in stale:
            deal = self.session.query(Deal).filter(Deal.id == fu.deal_id).first() if fu.deal_id else None
            assignee = deal.assignee if deal else None
            days_stale = (datetime.utcnow() - fu.sent_at).days if fu.sent_at else 0

            alert_msg = (
                f"⚠️ *ESCALAÇÃO — Sem Resposta*\n"
                f"Contato: {fu.target_person or fu.target_group}\n"
                f"Deal: {deal.name if deal else 'N/A'}\n"
                f"Dias sem resposta: {days_stale}\n"
                f"Mensagem enviada: {(fu.message or '')[:120]}...\n\n"
                f"Ação necessária: intervenção manual."
            )

            # Enviar alerta para o sócio responsável (ou todos se sem assignee)
            recipients = [socios_phones[assignee]] if assignee and assignee in socios_phones else list(socios_phones.values())
            recipients = [p for p in recipients if p]  # filtrar phones vazios

            for phone in recipients:
                if not dry_run:
                    try:
                        self.wpp.send(AgentRole.MANAGER, phone, alert_msg)
                    except Exception as exc:
                        logger.error("Falha ao escalonar para %s: %s", phone, exc)

            fu.status = "expirado"
            escalated_count += 1

            self.log_action("escalated", {
                "followup_id": fu.id,
                "target": fu.target_person,
                "days_stale": days_stale,
                "assignee": assignee,
            })

        return escalated_count

    # ──────────────────────────────────────────────────────────
    # Utilitário: criar follow-up programaticamente
    # ──────────────────────────────────────────────────────────

    def schedule_followup(
        self,
        deal_id: int,
        target_person: str,
        target_group: str,
        due_in_hours: int = 24,
        initial_message: str = "",
    ) -> FollowUp:
        """
        Agenda um novo follow-up para um deal específico.

        Args:
            deal_id:        ID do Deal no banco.
            target_person:  Número de telefone E.164 do contato.
            target_group:   Número/SID do grupo WhatsApp de destino.
            due_in_hours:   Horas até o prazo de envio (default: 24h).
            initial_message: Mensagem base (pode ser sobrescrita pelo Claude).

        Returns:
            Objeto FollowUp criado e persistido.
        """
        due_at = datetime.utcnow() + timedelta(hours=due_in_hours)
        fu = FollowUp(
            deal_id=deal_id,
            target_person=target_person,
            target_group=target_group,
            message=initial_message,
            due_at=due_at,
            status="pendente",
        )
        self.session.add(fu)
        self.session.commit()

        self.log_action("followup_scheduled", {
            "deal_id": deal_id,
            "target": target_person or target_group,
            "due_at": due_at.isoformat(),
        })
        return fu


# ── CLI standalone ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Samba Follow-Up Agent")
    parser.add_argument("--dry-run", action="store_true", help="Não envia mensagens WA")
    parser.add_argument("--check-responses", action="store_true", help="Só processa respostas recebidas")
    parser.add_argument("--max-batch", type=int, default=20, help="Máx envios por ciclo")
    args = parser.parse_args()

    agent = FollowUpAgent()
    result = agent.run({
        "dry_run": args.dry_run,
        "check_responses": args.check_responses,
        "max_batch": args.max_batch,
    })

    print(f"\n{'='*60}")
    print(f"Follow-Up Agent — Resultado")
    print(f"{'='*60}")
    print(f"Status:              {result.get('status')}")
    if args.dry_run:
        print(f"Simulados (dry-run): {result.get('simulated', 0)}")
    else:
        print(f"Enviados:            {result.get('sent', 0)}")
    print(f"Pulados:             {result.get('skipped', 0)}")
    print(f"Erros:               {result.get('errors', 0)}")
    print(f"Respostas recebidas: {result.get('responses_processed', 0)}")
    print(f"Escalados:           {result.get('escalated', 0)}")
