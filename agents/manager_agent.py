"""
agents/manager_agent.py
=======================
O CÉREBRO da Samba Export — Agente Gerente Geral.

Responsabilidades:
  (a) Lê todos os Deals do banco de dados
  (b) Classifica cada deal como COMPRA ou VENDA via Claude
  (c) Cruza ofertas para detectar matches/arbitragem:
      Fabrício vende soja MA a USD 450 + Vietnã paga USD 460 = spread USD 10/MT
  (d) Gera briefing diário consolidado para os sócios
  (e) Atribui deals aos sócios (Leonardo, Nivio, Marcelo) por commodity
  (f) Envia briefing via WhatsAppManager role=MANAGER

Uso:
    python agents/manager_agent.py
"""

from __future__ import annotations

import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from models.database import get_session, Deal, FollowUp
from services.gemini_api import ask_gemini as ask_claude, ask_gemini_json as ask_claude_json, MODEL_DEEP, MODEL_FAST
from services.whatsapp_api import get_whatsapp_manager, AgentRole

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)


# ── Configuração dos sócios ──────────────────────────────────────────────────
# Cada sócio recebe deals de commodities específicas por especialidade
SOCIO_COMMODITIES: dict[str, list[str]] = {
    "Leonardo": ["soja", "milho", "trigo", "arroz", "feijão", "sorgo"],
    "Nivio":    ["açúcar", "sugar", "etanol", "ethanol", "algodão", "cotton"],
    "Marcelo":  ["café", "coffee", "cacau", "cocoa", "frango", "chicken", "boi", "beef"],
}

# Sócio padrão para commodities não mapeadas
SOCIO_DEFAULT = "Leonardo"

# System prompt especializado para o Manager — será cacheado
_MANAGER_SYSTEM = """Você é o Gerente Geral de Inteligência Comercial da Samba Export.
Sua função é analisar o pipeline de deals de trading de commodities e gerar insights estratégicos.

Contexto operacional:
- Samba Export atua como trading company: compra commodities de produtores/exportadores brasileiros
  e vende para importadores internacionais (especialmente Ásia, África e Oriente Médio).
- Os deals vêm de grupos WhatsApp monitorados pelo Agente Extrator.
- Oportunidade de arbitragem = quando um vendedor oferece preço X e um comprador paga X + spread.
- Margem mínima aceitável: USD 3/MT para grãos, USD 5/MT para proteínas, USD 8/MT para café.

Sócios responsáveis:
- Leonardo: Grãos (Soja, Milho, Trigo, Arroz)
- Nivio: Industriais (Açúcar, Etanol, Algodão)
- Marcelo: Proteínas e Sabores (Café, Frango, Boi, Cacau)

Responda sempre em português brasileiro, de forma direta e objetiva."""


class ManagerAgent(BaseAgent):
    """
    Agente Gerente — processa o pipeline completo, detecta arbitragem,
    gera briefing e distribui deals aos sócios via WhatsApp.
    """

    name = "ManagerAgent"
    description = (
        "Cérebro da operação: cruza ofertas de compra/venda, "
        "detecta arbitragem e distribui deals aos sócios."
    )
    visible_in_groups = False       # Não entra em grupos externos
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
        Executa o ciclo completo do Manager:
          1. Carrega deals do banco
          2. Classifica buy/sell via Claude
          3. Detecta matches/arbitragem
          4. Atribui deals aos sócios
          5. Gera briefing
          6. Envia via WhatsApp

        Args:
            data: dict opcional com chaves:
                - mode: "full" (padrão) | "briefing_only" | "assign_only"
                - dry_run: bool (não envia WA se True)
        """
        if data is None:
            data = {}

        mode = data.get("mode", "full")
        dry_run = data.get("dry_run", False)
        limit = data.get("limit", None)  # limita deals para testes

        self.log_action("cycle_started", {"mode": mode, "dry_run": dry_run, "limit": limit})

        # 1. Carregar deals ativos
        deals = self._load_active_deals(limit=limit)
        if not deals:
            self.log_action("no_deals", level="WARNING")
            return {"status": "skipped", "reason": "Nenhum deal ativo no banco."}

        self.log_action("deals_loaded", {"count": len(deals)})

        results: dict[str, Any] = {
            "deals_total": len(deals),
            "assignments": [],
            "matches": [],
            "briefing_sent": False,
        }

        # 2. Atribuir deals aos sócios (se ainda sem assignee)
        if mode in ("full", "assign_only"):
            assignments = self._assign_deals(deals)
            results["assignments"] = assignments
            self.log_action("assignments_done", {"count": len(assignments)})

        # 3. Detectar matches e arbitragem
        if mode in ("full", "briefing_only"):
            classified = self._classify_deals_direction(deals)
            matches = self._detect_matches(classified)
            results["matches"] = matches
            self.log_action("matches_found", {"count": len(matches)})

            # Persiste matches detectados nas notas dos deals (não perde entre runs)
            if matches and not dry_run:
                persisted = self._persist_matches(matches)
                self.log_action("matches_persisted", {"count": persisted})

            # 4. Gerar briefing diário
            briefing = self._generate_briefing(deals, matches)
            results["briefing_preview"] = briefing[:300] + "..." if len(briefing) > 300 else briefing

            # 5. Enviar briefing aos sócios
            if not dry_run:
                send_results = self._send_briefing_to_socios(briefing)
                results["briefing_sent"] = True
                results["send_results"] = send_results
                self.log_action("briefing_sent", {"recipients": len(send_results)})
            else:
                logger.info("[DRY RUN] Briefing não enviado.\n%s", briefing)

        self.session.commit()
        return results

    # ──────────────────────────────────────────────────────────
    # (a) Carregar deals
    # ──────────────────────────────────────────────────────────

    def _load_active_deals(self, limit: int = None) -> list[Deal]:
        """Retorna deals com status 'ativo' do banco (apenas com preço para arbitragem)."""
        q = (
            self.session.query(Deal)
            .filter(Deal.status == "ativo")
            .order_by(Deal.created_at.desc())
        )
        if limit:
            q = q.limit(limit)
        return q.all()

    # ──────────────────────────────────────────────────────────
    # (b) Classificar direção: COMPRA vs VENDA
    # ──────────────────────────────────────────────────────────

    def _classify_deals_direction(self, deals: list[Deal]) -> list[dict]:
        """
        Classifica cada deal como 'compra' ou 'venda' usando Claude (rápido).
        Também extrai porto de origem/destino se ainda não preenchido.

        Retorna lista de dicts enriquecidos com a chave 'direction'.
        """
        classified = []
        for deal in deals:
            direction = self._classify_single_deal(deal)
            classified.append({
                "id": deal.id,
                "name": deal.name,
                "commodity": deal.commodity or "Desconhecida",
                "price": deal.price,
                "currency": deal.currency or "USD",
                "volume": deal.volume,
                "volume_unit": deal.volume_unit or "MT",
                "incoterm": deal.incoterm,
                "origin": deal.origin,
                "destination": deal.destination,
                "stage": deal.stage,
                "assignee": deal.assignee,
                "source_sender": deal.source_sender,
                "source_group": deal.source_group,
                "direction": direction,
                "risk_score": deal.risk_score or 50,
                "notes": (deal.notes or "")[:200],
            })
        return classified

    def _classify_single_deal(self, deal: Deal) -> str:
        """
        Classifica se o deal é VENDA ou COMPRA.
        Usa heurísticas multi-campo antes de recorrer ao Gemini.
        Retorna "venda", "compra" ou "indefinido".
        """
        # 0. Já classificado anteriormente — reutiliza
        if deal.direcao and deal.direcao not in ("UNKNOWN", ""):
            return deal.direcao.lower()

        # 1. Combina todos os campos textuais para análise
        hint = " ".join(filter(None, [
            deal.name or "",
            deal.notes or "",
            deal.source_group or "",
            deal.origin or "",
            deal.destination or "",
            deal.source_sender or "",
        ])).lower()

        # 2. Keywords de VENDA
        for kw in ["vend", "offer", "disponivel", "disponível", "fob origin",
                   "exporta", "produt", "selling", "for sale", "estoque",
                   "lote disponív", "oferta", "we have", "temos disponív",
                   "prazo de entrega"]:
            if kw in hint:
                return "venda"

        # 3. Keywords de COMPRA
        for kw in ["compr", "need", "procur", "demand", "want", "busca",
                   "importa", "buying", "looking for", "preciso", "precisa",
                   "quero comprar", "necessito", "rfq", "request for"]:
            if kw in hint:
                return "compra"

        # 4. Heurística por Incoterm
        if deal.incoterm:
            inco = deal.incoterm.upper()
            if inco in ("FOB", "FAS", "EXW") and deal.origin:
                return "venda"   # Exportador define o incoterm de saída
            if inco in ("CIF", "CFR", "DAP", "DDP") and deal.destination:
                return "compra"  # Comprador especifica entrega no destino

        # 5. Sem informação suficiente — retorna "indefinido" sem chamar API
        #    (evita quota para ~80% dos deals sem incoterm/keywords)
        return "indefinido"

    # ──────────────────────────────────────────────────────────
    # (c) Detectar matches / arbitragem
    # ──────────────────────────────────────────────────────────

    def _detect_matches(self, classified_deals: list[dict]) -> list[dict]:
        """
        Cruza vendedores com compradores da mesma commodity.

        Lógica:
          Para cada par (vendedor, comprador) da mesma commodity:
            - Se preço_comprador > preço_vendedor → spread positivo → match!
            - Spread mínimo por commodity configurado em MIN_SPREAD

        Retorna lista de oportunidades ordenadas por spread decrescente.
        """
        MIN_SPREAD: dict[str, float] = {
            "soja": 3.0, "milho": 3.0, "trigo": 3.0, "arroz": 3.0,
            "açúcar": 3.0, "sugar": 3.0, "etanol": 0.05,
            "algodão": 5.0, "cotton": 5.0,
            "café": 8.0, "coffee": 8.0, "cacau": 8.0,
            "frango": 5.0, "chicken": 5.0, "boi": 5.0,
        }

        # Agrupar por commodity
        by_commodity: dict[str, dict[str, list[dict]]] = defaultdict(lambda: {"venda": [], "compra": []})
        for deal in classified_deals:
            comm = (deal["commodity"] or "").lower()
            direction = deal["direction"]
            if direction in ("venda", "compra") and deal["price"]:
                by_commodity[comm][direction].append(deal)

        matches = []
        for commodity, groups in by_commodity.items():
            vendedores = groups["venda"]
            compradores = groups["compra"]
            min_spread = MIN_SPREAD.get(commodity, 3.0)

            for vendedor in vendedores:
                for comprador in compradores:
                    if vendedor["currency"] != comprador["currency"]:
                        continue  # evitar comparações cross-currency sem câmbio

                    preco_venda = vendedor["price"]
                    preco_compra = comprador["price"]
                    spread = preco_compra - preco_venda

                    if spread >= min_spread:
                        matches.append({
                            "commodity": commodity,
                            "vendedor_id": vendedor["id"],
                            "vendedor_nome": vendedor["source_sender"],
                            "vendedor_grupo": vendedor["source_group"],
                            "preco_venda": preco_venda,
                            "comprador_id": comprador["id"],
                            "comprador_nome": comprador["source_sender"],
                            "comprador_grupo": comprador["source_group"],
                            "preco_compra": preco_compra,
                            "spread": round(spread, 2),
                            "currency": vendedor["currency"],
                            "volume_ref": min(
                                vendedor["volume"] or 0,
                                comprador["volume"] or 0,
                            ) or None,
                            "incoterm_venda": vendedor["incoterm"],
                            "incoterm_compra": comprador["incoterm"],
                        })

        # Ordenar por maior spread
        matches.sort(key=lambda m: m["spread"], reverse=True)
        self.log_action("matches_detected", {
            "total_matches": len(matches),
            "commodities": list({m["commodity"] for m in matches}),
        })
        return matches

    # ──────────────────────────────────────────────────────────
    # (c.2) Persistir matches detectados no banco
    # ──────────────────────────────────────────────────────────

    def _persist_matches(self, matches: list[dict]) -> int:
        """
        Anota as oportunidades de arbitragem detectadas nas notas dos deals
        participantes (vendedor e comprador), evitando append duplicado.

        Retorna o número de deals atualizados.
        """
        today_str = datetime.now().strftime("%d/%m/%Y")
        updated = 0

        for m in matches:
            tag = f"[MATCH {today_str}]"
            match_line = (
                f"{tag} {m['commodity'].upper()} spread "
                f"{m['currency']} {m['spread']:,.2f}/MT | "
                f"Venda {m['preco_venda']:,.2f} × Compra {m['preco_compra']:,.2f}"
            )

            for deal_id, side in [
                (m["vendedor_id"], "VENDEDOR"),
                (m["comprador_id"], "COMPRADOR"),
            ]:
                if not deal_id:
                    continue
                deal = self.session.query(Deal).filter(Deal.id == deal_id).first()
                if not deal:
                    continue

                # Evita duplicata no mesmo dia
                existing_notes = deal.notes or ""
                if tag in existing_notes:
                    continue

                counterpart = (
                    m["comprador_nome"] if side == "VENDEDOR"
                    else m["vendedor_nome"]
                )
                full_line = f"{match_line} | Contraparte: {counterpart} ({side})"
                deal.notes = (existing_notes + "\n" + full_line).strip()
                deal.updated_at = datetime.utcnow()
                updated += 1
                logger.info("Match persistido: Deal #%d — %s", deal_id, full_line[:80])

        if updated:
            self.session.commit()

        return updated

    # ──────────────────────────────────────────────────────────
    # (d) Gerar briefing diário
    # ──────────────────────────────────────────────────────────

    def _generate_briefing(self, deals: list[Deal], matches: list[dict]) -> str:
        """
        Usa Claude Opus para gerar o briefing diário consolidado.
        Inclui: pipeline resumido, matches/arbitragem, alertas de risco.
        """
        today = datetime.now().strftime("%d/%m/%Y %H:%M")

        # Resumo do pipeline por estágio
        stage_counts: dict[str, int] = defaultdict(int)
        for deal in deals:
            stage_counts[deal.stage or "Indefinido"] += 1

        # Top 5 deals por risco
        top_risk = sorted(
            [d for d in deals if d.risk_score],
            key=lambda d: d.risk_score or 0,
            reverse=True,
        )[:5]

        risk_summary = "\n".join(
            f"  - {d.name} | Risco: {d.risk_score}/100 | Sócio: {d.assignee or 'não atribuído'}"
            for d in top_risk
        )

        matches_summary = ""
        if matches:
            matches_summary = "\n\nOPORTUNIDADES DE ARBITRAGEM DETECTADAS:\n"
            for m in matches[:5]:  # Top 5 matches
                vol_str = f" | Vol ref: {m['volume_ref']:,} {'' }" if m["volume_ref"] else ""
                matches_summary += (
                    f"  ★ {m['commodity'].upper()} | "
                    f"Venda {m['currency']} {m['preco_venda']:,.2f} ({m['vendedor_nome']}) × "
                    f"Compra {m['currency']} {m['preco_compra']:,.2f} ({m['comprador_nome']}) "
                    f"→ SPREAD {m['currency']} {m['spread']:,.2f}/MT{vol_str}\n"
                )
        else:
            matches_summary = "\n\nNenhum match de arbitragem identificado hoje."

        prompt = f"""Gere o briefing diário da Samba Export para os sócios.

Data: {today}

PIPELINE ATUAL ({len(deals)} deals ativos):
{json.dumps(dict(stage_counts), ensure_ascii=False)}

TOP 5 DEALS DE MAIOR RISCO:
{risk_summary or 'Nenhum deal de alto risco'}
{matches_summary}

Gere um briefing executivo em formato WhatsApp (máximo 25 linhas) incluindo:
1. Saudação diária com a data
2. Resumo do pipeline (1-2 frases com números)
3. Destaques de arbitragem (se houver) — seja direto: "Match: [commodity] spread USD X/MT"
4. Alertas de deals de risco crítico (>75/100)
5. Call-to-action para os sócios (o que precisa de ação hoje)
6. Assinatura: "🎷 Samba Manager | Agente IA"

Use emojis estrategicamente. Tom profissional mas ágil. Português brasileiro."""

        briefing = ask_claude(
            prompt,
            system=_MANAGER_SYSTEM,
            model=MODEL_DEEP,
            max_tokens=1024,
        )

        self.log_action("briefing_generated", {"chars": len(briefing)})
        return briefing

    # ──────────────────────────────────────────────────────────
    # (e) Atribuir deals aos sócios
    # ──────────────────────────────────────────────────────────

    def _assign_deals(self, deals: list[Deal]) -> list[dict]:
        """
        Atribui deals sem assignee ao sócio responsável pela commodity.
        Persiste a atribuição no banco.

        Regras:
          - Leonardo → Grãos (Soja, Milho, Trigo, Arroz, Feijão)
          - Nivio    → Industriais (Açúcar, Etanol, Algodão)
          - Marcelo  → Proteínas/Sabores (Café, Frango, Boi, Cacau)
          - Indefinido → Leonardo (padrão)
        """
        assignments = []
        for deal in deals:
            if deal.assignee:
                continue  # já atribuído, não sobrescrever

            sócio = self._resolve_assignee(deal.commodity)
            deal.assignee = sócio
            deal.updated_at = datetime.utcnow()

            assignments.append({
                "deal_id": deal.id,
                "deal_name": deal.name,
                "commodity": deal.commodity,
                "assignee": sócio,
            })
            self.log_action("deal_assigned", {
                "deal_id": deal.id,
                "commodity": deal.commodity,
                "assignee": sócio,
            })

        return assignments

    def _resolve_assignee(self, commodity: str | None) -> str:
        """Retorna o nome do sócio responsável pela commodity."""
        if not commodity:
            return SOCIO_DEFAULT
        comm_lower = commodity.lower()
        for socio, keywords in SOCIO_COMMODITIES.items():
            for kw in keywords:
                if kw in comm_lower:
                    return socio
        return SOCIO_DEFAULT

    # ──────────────────────────────────────────────────────────
    # (f) Enviar briefing via WhatsApp
    # ──────────────────────────────────────────────────────────

    def _send_briefing_to_socios(self, briefing: str) -> list[dict]:
        """
        Envia o briefing para o número pessoal de cada sócio
        via WhatsAppManager role=MANAGER.

        Lê os números das env vars SOCIO_1_PHONE, SOCIO_2_PHONE, SOCIO_3_PHONE.
        """
        import os
        socios = [
            {"name": os.getenv("SOCIO_1_NAME", "Leonardo"), "phone": os.getenv("SOCIO_1_PHONE", "")},
            {"name": os.getenv("SOCIO_2_NAME", "Nivio"),    "phone": os.getenv("SOCIO_2_PHONE", "")},
            {"name": os.getenv("SOCIO_3_NAME", "Marcelo"),  "phone": os.getenv("SOCIO_3_PHONE", "")},
        ]

        results = []
        for socio in socios:
            phone = socio["phone"]
            if not phone:
                logger.warning("Telefone não configurado para sócio: %s", socio["name"])
                results.append({"socio": socio["name"], "status": "skipped", "reason": "no_phone"})
                continue

            # Personalizar abertura para o sócio
            msg = f"Bom dia, {socio['name']}!\n\n{briefing}"

            try:
                send_result = self.wpp.send(AgentRole.MANAGER, phone, msg)
                send_result["socio"] = socio["name"]
                results.append(send_result)
                self.log_action("wpp_sent", {
                    "socio": socio["name"],
                    "phone": phone,
                    "sid": send_result.get("sid"),
                })
            except Exception as exc:
                logger.error("Falha ao enviar WA para %s: %s", socio["name"], exc)
                results.append({"socio": socio["name"], "status": "error", "error": str(exc)})

        return results


# ── CLI standalone ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Samba Manager Agent")
    parser.add_argument("--mode", default="full", choices=["full", "briefing_only", "assign_only"])
    parser.add_argument("--dry-run", action="store_true", help="Não envia mensagens WA")
    args = parser.parse_args()

    agent = ManagerAgent()
    result = agent.run({"mode": args.mode, "dry_run": args.dry_run})

    print(f"\n{'='*60}")
    print(f"Manager Agent — Resultado")
    print(f"{'='*60}")
    print(f"Status:      {result.get('status')}")
    print(f"Deals:       {result.get('deals_total', 0)}")
    print(f"Atribuições: {len(result.get('assignments', []))}")
    print(f"Matches:     {len(result.get('matches', []))}")

    if result.get("matches"):
        print(f"\nTop matches de arbitragem:")
        for m in result["matches"][:3]:
            print(f"  {m['commodity'].upper()} | spread {m['currency']} {m['spread']:.2f}/MT")

    print(f"\nBriefing enviado: {'Sim' if result.get('briefing_sent') else 'Não (dry-run ou erro)'}")
