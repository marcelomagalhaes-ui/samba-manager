"""
services/claude_api.py
======================
Wrapper da API Anthropic (Claude) para o Samba Export Control Desk.

Funções públicas:
  - ask_claude()                → resposta em texto simples
  - ask_claude_json()           → resposta como dict (JSON)
  - extract_quote_data()        → extrai commodity/preço/volume/incoterm de msg WhatsApp
  - analyze_deal_risk()         → score de risco 0-100 para um deal
  - generate_followup_message() → gera mensagem de follow-up para WhatsApp
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import anthropic
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ── Modelos ─────────────────────────────────────────────────────────────────
# claude-sonnet-4-6 → rápido, extrações e follow-ups
# claude-opus-4-6   → raciocínio profundo, análise de risco, decisões críticas
MODEL_FAST = "claude-sonnet-4-6"
MODEL_DEEP = "claude-opus-4-6"

# System prompt base — será cacheado na API (prompt caching)
_SAMBA_SYSTEM = """Você é um assistente especializado em comércio exterior de commodities agrícolas brasileiras.
Atua como analista sênior da Samba Export, empresa de trading de commodities focada em exportação.

Commodities principais: Soja, Milho, Café Arábica, Açúcar Cristal/VHP/ICUMSA, Etanol Hidratado,
Algodão, Boi Gordo, Trigo, Arroz, Frango (pé de frango, peito, asa), Cacau, Feijão.

Moedas: USD (padrão internacional), BRL (mercado interno).
Incoterms: FOB, CIF, FAS, CFR, DAP, DDP, EXW, ASWP.
Portos de referência: Santos (SP), Paranaguá (PR), Itaqui (MA), Vitória (ES), Rio Grande (RS).

Responda sempre em português brasileiro, de forma objetiva e direta."""


# ──────────────────────────────────────────────────────────────────────────────
# Client helper
# ──────────────────────────────────────────────────────────────────────────────

def _get_client() -> anthropic.Anthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY não definida. Adicione ao arquivo .env ou nas variáveis de ambiente."
        )
    return anthropic.Anthropic(api_key=api_key)


# ──────────────────────────────────────────────────────────────────────────────
# Funções públicas
# ──────────────────────────────────────────────────────────────────────────────

def ask_claude(
    prompt: str,
    system: str = _SAMBA_SYSTEM,
    model: str = MODEL_FAST,
    max_tokens: int = 2048,
) -> str:
    """
    Envia uma pergunta ao Claude e retorna a resposta como texto.

    O system prompt é enviado com cache_control para economizar tokens
    em chamadas repetidas (prompt caching da Anthropic).

    Args:
        prompt:     Mensagem / pergunta do usuário.
        system:     System prompt (usa o padrão Samba se omitido).
        model:      ID do modelo Claude a usar.
        max_tokens: Limite de tokens na resposta.

    Returns:
        String com a resposta do Claude.
    """
    client = _get_client()

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=[
            {
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral"},  # cache por 5 min
            }
        ],
        messages=[{"role": "user", "content": prompt}],
    )

    text = next((b.text for b in response.content if b.type == "text"), "")

    logger.debug(
        "ask_claude | model=%s tokens_in=%d tokens_out=%d cache_read=%d",
        model,
        response.usage.input_tokens,
        response.usage.output_tokens,
        getattr(response.usage, "cache_read_input_tokens", 0),
    )

    return text


def ask_claude_json(
    prompt: str,
    system: str = _SAMBA_SYSTEM,
    model: str = MODEL_FAST,
    max_tokens: int = 2048,
) -> dict:
    """
    Envia uma pergunta ao Claude e retorna a resposta como dict.

    O prompt deve instruir o Claude a responder APENAS com JSON válido.
    Esta função adiciona automaticamente essa instrução ao system prompt.

    Returns:
        dict com a resposta parseada.
        Em caso de falha no parse: {"error": "<msg>", "raw": "<texto>"}
    """
    json_suffix = (
        "\n\nIMPORTANTE: Responda SOMENTE com um objeto JSON válido. "
        "Sem markdown, sem explicações, sem blocos ```json```. Apenas o JSON puro."
    )

    client = _get_client()

    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=[
            {
                "type": "text",
                "text": system + json_suffix,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": prompt}],
    )

    raw = next((b.text for b in response.content if b.type == "text"), "{}").strip()

    # Limpar markdown caso o modelo inclua mesmo assim
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1].lstrip("json").strip() if len(parts) >= 2 else raw

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("JSON parse error: %s | raw=%s", e, raw[:300])
        return {"error": str(e), "raw": raw}


def extract_quote_data(
    message_text: str,
    sender: str = "",
    group: str = "",
) -> dict:
    """
    Extrai dados estruturados de cotação de uma mensagem WhatsApp.

    Usa claude-sonnet-4-6 (rápido) para processar em tempo real.

    Args:
        message_text: Conteúdo da mensagem.
        sender:       Nome do remetente (opcional, melhora contexto).
        group:        Nome do grupo (opcional, melhora contexto).

    Returns:
        dict com:
            commodity (str|None), price (float|None), currency (str|None),
            volume (float|None), volume_unit (str|None), incoterm (str|None),
            location (str|None), has_quote (bool), confidence (float 0-1)
    """
    prompt = f"""Analise esta mensagem de WhatsApp de trading de commodities e extraia os dados de cotação.

Remetente: {sender or "Desconhecido"}
Grupo: {group or "Desconhecido"}

Mensagem:
---
{message_text[:1000]}
---

Extraia e retorne um JSON com os seguintes campos:
- "commodity": nome padronizado da commodity (ex: "Soja", "Açúcar VHP", "Milho") ou null
- "price": valor numérico do preço (apenas o número, sem moeda) ou null
- "currency": "USD" ou "BRL" (inferir pelo contexto) ou null
- "volume": valor numérico do volume ou null
- "volume_unit": "MT", "SC" (sacas), "M3", "L", "KG" etc ou null
- "incoterm": ex "FOB", "CIF", "FAS" ou null
- "location": porto/cidade de referência (ex: "Santos", "Paranaguá") ou null
- "has_quote": true se contém uma cotação/oferta real de preço ou volume
- "confidence": número de 0.0 a 1.0 indicando confiança na extração"""

    result = ask_claude_json(prompt, model=MODEL_FAST, max_tokens=512)

    # Garantir todos os campos com defaults
    defaults: dict[str, Any] = {
        "commodity": None,
        "price": None,
        "currency": None,
        "volume": None,
        "volume_unit": None,
        "incoterm": None,
        "location": None,
        "has_quote": False,
        "confidence": 0.0,
    }
    defaults.update({k: v for k, v in result.items() if k in defaults})
    return defaults


def analyze_deal_risk(deal_data: dict) -> dict:
    """
    Analisa o risco de um deal comercial e retorna um score 0-100.

    Usa claude-opus-4-6 (raciocínio mais profundo) para avaliação.

    Args:
        deal_data: dict descrevendo o deal. Campos relevantes:
            commodity, volume, volume_unit, price, currency, incoterm,
            origin, destination, stage, counterparty, notes, etc.

    Returns:
        dict com:
            score (int 0-100), level ("baixo"|"médio"|"alto"|"crítico"),
            factors (list[str]), recommendation (str)
    """
    prompt = f"""Avalie o risco deste deal de exportação de commodities.

Dados do deal:
{json.dumps(deal_data, ensure_ascii=False, indent=2)}

Analise os seguintes fatores de risco:
1. Risco de contraparte (reputação, solidez financeira da empresa compradora)
2. Risco de mercado (volatilidade de preço da commodity no momento)
3. Risco logístico (complexidade da rota origem → destino, modal utilizado)
4. Risco documental (incoterm escolhido, complexidade dos documentos exigidos)
5. Risco cambial (exposição a variação USD/BRL ou outra moeda)
6. Risco de volume (tamanho do deal vs capacidade operacional)
7. Risco de estágio (quão avançado está no pipeline e tempo decorrido)

Retorne JSON com:
- "score": inteiro de 0 (sem risco) a 100 (risco crítico)
- "level": "baixo" (0-25), "médio" (26-50), "alto" (51-75) ou "crítico" (76-100)
- "factors": lista de strings descrevendo os 3-5 principais fatores de risco identificados
- "recommendation": string com 1-2 frases de ação imediata recomendada"""

    result = ask_claude_json(prompt, model=MODEL_DEEP, max_tokens=1024)

    # Normalizar score
    raw_score = result.get("score", 50)
    try:
        score = max(0, min(100, int(float(raw_score))))
    except (TypeError, ValueError):
        score = 50

    result["score"] = score

    # Garantir level consistente com score
    if "level" not in result or not result["level"]:
        if score <= 25:
            result["level"] = "baixo"
        elif score <= 50:
            result["level"] = "médio"
        elif score <= 75:
            result["level"] = "alto"
        else:
            result["level"] = "crítico"

    result.setdefault("factors", [])
    result.setdefault("recommendation", "Revisar deal manualmente.")

    return result


def generate_followup_message(context: dict) -> str:
    """
    Gera uma mensagem de follow-up para WhatsApp baseada no contexto do deal.

    Args:
        context: dict com informações do contato/deal. Campos úteis:
            - contact_name (str): nome do contato
            - commodity (str): commodity em negociação
            - last_interaction (str): descrição da última interação
            - days_since_contact (int): dias sem resposta
            - deal_stage (str): estágio atual ("Lead", "Negociação", etc.)
            - price_discussed (float|None): preço discutido anteriormente
            - volume (str|None): volume em negociação
            - tone (str): "formal" | "informal" (default: "informal")

    Returns:
        Texto pronto para enviar no WhatsApp.
    """
    tone = context.get("tone", "informal")
    tone_guide = (
        "Tom INFORMAL e natural, como colegas de mercado. Sem formalidades excessivas."
        if tone == "informal"
        else "Tom FORMAL e profissional. Tratamento respeitoso."
    )

    prompt = f"""Crie uma mensagem de follow-up para WhatsApp para retomar um negócio de commodities.

Contexto do contato/deal:
{json.dumps(context, ensure_ascii=False, indent=2)}

Diretrizes:
- {tone_guide}
- Máximo 3-4 frases curtas (mensagem de WhatsApp, não e-mail)
- Referencie naturalmente a commodity e/ou negociação anterior
- Inclua um call-to-action claro: resposta, reunião, nova proposta ou confirmação
- Português brasileiro natural, sem gírias excessivas
- NÃO use "Espero que esteja bem" ou frases genéricas de abertura
- NÃO use emojis em excesso (máximo 1 se o tom for informal)
- Retorne APENAS o texto da mensagem, sem aspas, sem título, sem explicação adicional"""

    return ask_claude(prompt, model=MODEL_FAST, max_tokens=256)


# ── System prompt para documentação jurídica ─────────────────────────────────
_DOCUMENTAL_SYSTEM = """Você é um especialista jurídico em contratos de exportação de commodities agrícolas brasileiras.
Atua como advogado sênior da Samba Export, com profundo conhecimento em:

- Contratos de compra e venda internacional (SCO — Sales Confirmation Order)
- Contratos firmes de commodities (FCO — Full Corporate Offer)
- Acordos de não divulgação e não circunvenção (NCNDA — Non-Circumvention, Non-Disclosure Agreement)
- Legislação brasileira de comércio exterior (Lei 9.280/96, SISCOMEX, Registro de Exportadores)
- Incoterms 2020 (ICC) e sua aplicação prática
- Câmbio e contratos de câmbio (BACEN, regulamentação LGPD aplicável)
- UCP 600 (Cartas de Crédito Documentário)

Redija minutas precisas, claras e juridicamente válidas.
Substitua lacunas de informação por placeholders no formato [CAMPO_A_PREENCHER].
Responda sempre em português jurídico brasileiro formal."""


def generate_document_draft(deal_data: dict, doc_type: str = "SCO") -> str:
    """
    Gera uma minuta jurídica inicial baseada nos dados do deal.

    Usa claude-opus-4-6 para precisão jurídica máxima.

    Args:
        deal_data: dict com dados do deal. Campos esperados:
            - Commodity (str)
            - Volume (str): ex "5000 MT"
            - Preço (str): ex "USD 285.00"
            - Incoterm (str): ex "FOB Santos"
            - Origem/Porto (str)
            - Comprador (WhatsApp) (str): remetente/contato
            - Contexto_Juridico_Drive (str): trecho do template do Drive (pode ser "")
        doc_type: Tipo de documento:
            - "SCO"   → Sales Confirmation Order (exportação USD)
            - "FCO"   → Full Corporate Offer / Contrato Firme (BRL mercado interno)
            - "NCNDA" → Non-Circumvention Non-Disclosure Agreement

    Returns:
        Texto completo da minuta em formato pronto para revisão.
    """
    doc_descriptions = {
        "SCO": "Sales Confirmation Order (SCO) — Confirmação de Venda Internacional",
        "FCO": "Full Corporate Offer (FCO) / Contrato de Compra e Venda de Commodities",
        "NCNDA": "Non-Circumvention, Non-Disclosure Agreement (NCNDA)",
    }
    doc_label = doc_descriptions.get(doc_type, f"Contrato {doc_type}")

    context_section = ""
    if deal_data.get("Contexto_Juridico_Drive"):
        context_section = f"""
Modelo de referência extraído do Google Drive (use como base estrutural):
---
{deal_data['Contexto_Juridico_Drive']}
---
"""

    prompt = f"""Redija uma minuta de {doc_label} com base nos dados abaixo.

DADOS DO NEGÓCIO:
{json.dumps({k: v for k, v in deal_data.items() if k != 'Contexto_Juridico_Drive'},
            ensure_ascii=False, indent=2)}
{context_section}
INSTRUÇÕES:
1. Use os dados fornecidos para preencher as cláusulas relevantes
2. Onde faltarem informações, use placeholders claros: [NOME_COMPRADOR], [CNPJ_EXPORTADOR], [DATA_ENTREGA], etc.
3. Inclua as seguintes seções obrigatórias para {doc_type}:
   {"- Partes (Vendedor/Comprador), Objeto, Quantidade e Qualidade, Preço e Condições de Pagamento, Entrega e Incoterm, Documentos, Penalidades, Foro" if doc_type in ("SCO", "FCO") else "- Partes, Objeto, Obrigações de Confidencialidade, Não-Circunvenção, Prazo de Vigência, Penalidades, Lei Aplicável"}
4. Ao final, adicione uma nota: "RASCUNHO PARA REVISÃO — Gerado automaticamente pela Samba Export AI. Sujeito à revisão jurídica antes de assinatura."
5. Use linguagem jurídica formal e precisa

Retorne APENAS o texto completo da minuta, sem comentários adicionais."""

    return ask_claude(prompt, system=_DOCUMENTAL_SYSTEM, model=MODEL_DEEP, max_tokens=4096)
