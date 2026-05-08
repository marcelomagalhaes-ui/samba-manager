"""
sync/exceptions.py
==================
Hierarquia única para o orquestrador.

Regra inviolável: qualquer `LLMUnavailable` (em qualquer uma das subclasses)
deve impedir a geração do PDF e marcar a linha como PENDING_IA na planilha.
"""


class LLMUnavailable(Exception):
    """Superclasse — orquestrador trata todas uniformemente como PENDING_IA."""


class LLMTransportError(LLMUnavailable):
    """Falha de rede / 429 / 503 / 500 — transitória, candidata a retry."""


class LLMDegradedResponse(LLMUnavailable):
    """A chamada retornou, mas o conteúdo é inútil (JSON inválido, dict vazio)."""


class CircuitBreakerOpen(LLMUnavailable):
    """Breaker aberto — falha rápido sem sequer tentar a API."""
