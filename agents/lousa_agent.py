"""
agents/lousa_agent.py
=====================
Agente Lousa (Whiteboard) da Samba Export.
Processa textos livres com diferentes modos de análise via Claude API.

Modos disponíveis:
  - Resumir
  - Traduzir para Inglês
  - Traduzir para Português
  - Extrair informações
  - Análise livre
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_MODOS = [
    "Resumir",
    "Traduzir para Inglês",
    "Traduzir para Português",
    "Extrair informações",
    "Análise livre",
]

_SYSTEM_BASE = """Você é um assistente da Samba Export, trading company especializada em
exportação de commodities agrícolas. Você auxilia a equipe com análise de textos
comerciais, documentos, mensagens e conteúdos relacionados ao comércio exterior.
Seja objetivo, profissional e direto."""

_PROMPTS = {
    "Resumir": (
        "Faça um resumo claro e conciso do texto abaixo. "
        "Destaque os pontos mais importantes em tópicos quando aplicável.\n\n"
        "TEXTO:\n{texto}"
    ),
    "Traduzir para Inglês": (
        "Traduza o texto abaixo para o inglês. Mantenha o tom profissional "
        "e o vocabulário técnico adequado para comércio exterior.\n\n"
        "TEXTO:\n{texto}"
    ),
    "Traduzir para Português": (
        "Traduza o texto abaixo para o português brasileiro. Mantenha o tom "
        "profissional e o vocabulário técnico adequado para comércio exterior.\n\n"
        "TEXTO:\n{texto}"
    ),
    "Extrair informações": (
        "Extraia e organize as informações mais relevantes do texto abaixo. "
        "Apresente em formato estruturado: commodity, volume, preço, incoterm, "
        "partes envolvidas, datas e quaisquer outros dados comerciais relevantes. "
        "Use 'N/A' para campos não identificados.\n\n"
        "TEXTO:\n{texto}"
    ),
    "Análise livre": (
        "Analise o texto abaixo do ponto de vista comercial da Samba Export. "
        "Identifique oportunidades, riscos, pontos de atenção e próximos passos "
        "recomendados.\n\n"
        "TEXTO:\n{texto}"
    ),
}


def get_modos() -> list[str]:
    """Retorna lista de modos disponíveis."""
    return _MODOS


def analisar_texto(
    texto: str,
    modo: str,
    model: str = "claude-haiku-4-5",
) -> str:
    """
    Analisa texto via Claude API com o modo especificado.

    Args:
        texto: Texto de entrada para análise
        modo:  Modo de análise (um dos valores em _MODOS)
        model: Modelo Claude a usar

    Returns:
        Resultado da análise como string.
    """
    if not texto or not texto.strip():
        return "Nenhum texto fornecido para análise."

    prompt_template = _PROMPTS.get(modo, _PROMPTS["Análise livre"])
    prompt = prompt_template.format(texto=texto.strip())

    try:
        import anthropic
        client = anthropic.Anthropic(timeout=60.0)   # 60s — análise de texto pode ser demorada
        response = client.messages.create(
            model=model,
            max_tokens=1500,
            system=_SYSTEM_BASE,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as exc:
        logger.exception("analisar_texto falhou")
        return f"Erro ao processar análise: {exc}"
