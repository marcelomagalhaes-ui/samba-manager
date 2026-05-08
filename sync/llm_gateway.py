"""
sync/llm_gateway.py
===================
Ponto único de entrada para chamadas ao LLM (Gemini).

Camadas de defesa (fora para dentro):
  1. Circuit Breaker — depois de N falhas consecutivas, fica OPEN por um cooldown.
     Chamadas durante OPEN falham imediato (CircuitBreakerOpen) sem tocar na API.
  2. Tenacity — retry exponencial + jitter para falhas transitórias.
  3. Normalização — a resposta do gemini_api ora é dict, ora list, ora um dict
     de erro (`{"error": ..., "raw": ...}`). Aqui unificamos.

Contrato: ou devolve um `dict` útil, ou levanta `LLMUnavailable`.
Nada de "conteúdo meio-pronto" atravessando — o orquestrador confia no tipo.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from services.gemini_api import extract_quote_data as _default_extract
from sync.exceptions import (
    CircuitBreakerOpen,
    LLMDegradedResponse,
    LLMTransportError,
    LLMUnavailable,
)

logger = logging.getLogger("SambaLLMGateway")


# ----------------------------------------------------------------------------
# Circuit Breaker — estado mutável e mínimo (single-threaded por enquanto)
# ----------------------------------------------------------------------------

@dataclass
class CircuitBreaker:
    """
    Três estados:
      - CLOSED   (opened_at=None): chamadas normais.
      - OPEN     (opened_at=now, dentro do cooldown): fail-fast.
      - HALF_OPEN (opened_at=old, fora do cooldown): a próxima chamada testa a API;
                 sucesso fecha o breaker, falha reabre.
    """
    failure_threshold: int = 3
    cooldown_seconds: float = 300.0  # 5 min
    consecutive_failures: int = 0
    opened_at: Optional[float] = None

    def is_open(self, now: float) -> bool:
        if self.opened_at is None:
            return False
        return (now - self.opened_at) < self.cooldown_seconds

    def remaining_cooldown(self, now: float) -> float:
        if self.opened_at is None:
            return 0.0
        return max(0.0, self.cooldown_seconds - (now - self.opened_at))

    def record_success(self) -> None:
        if self.opened_at is not None:
            logger.info("🟢 Breaker RECUPERADO após sucesso em HALF_OPEN.")
        self.consecutive_failures = 0
        self.opened_at = None

    def record_failure(self, now: float) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold and self.opened_at is None:
            self.opened_at = now
            logger.warning(
                "🔴 Breaker ABERTO. %d falhas consecutivas. Cooldown: %.0fs.",
                self.consecutive_failures,
                self.cooldown_seconds,
            )


# ----------------------------------------------------------------------------
# Gateway
# ----------------------------------------------------------------------------

class LLMGateway:
    """Chamada resiliente e padronizada para extração de dados via Gemini."""

    def __init__(
        self,
        extract_fn: Callable[..., Any] = _default_extract,
        breaker: Optional[CircuitBreaker] = None,
        max_attempts: int = 2,
        base_wait: float = 2.0,
        max_wait: float = 8.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._extract = extract_fn
        self.breaker = breaker or CircuitBreaker()
        self._clock = clock
        self._max_attempts = max_attempts
        self._base_wait = base_wait
        self._max_wait = max_wait

    # --- API pública ---------------------------------------------------------

    def extract_quote(
        self,
        *,
        message_text: str,
        sender: str = "",
        group: str = "",
    ) -> dict:
        """
        Ou devolve um dict útil, ou levanta uma subclasse de `LLMUnavailable`.
        O orquestrador usa um único `except LLMUnavailable` e marca PENDING_IA.
        """
        now = self._clock()
        if self.breaker.is_open(now):
            remaining = int(self.breaker.remaining_cooldown(now))
            raise CircuitBreakerOpen(
                f"Breaker aberto. Cooldown restante ~{remaining}s — "
                f"pulando chamada ao Gemini."
            )

        try:
            result = self._call_with_retry(message_text, sender, group)
        except LLMUnavailable:
            self.breaker.record_failure(self._clock())
            raise
        except Exception as e:
            # Proteção defensiva — normaliza exceção desconhecida.
            self.breaker.record_failure(self._clock())
            raise LLMTransportError(f"Falha inesperada no gateway: {e}") from e

        self.breaker.record_success()
        return result

    # --- Internos ------------------------------------------------------------

    def _call_with_retry(self, message_text: str, sender: str, group: str) -> dict:
        """
        Usa `Retrying` (modo imperativo do tenacity) para permitir injetar
        timings diferentes no construtor e testes.
        """
        retrying = Retrying(
            retry=retry_if_exception_type(LLMTransportError),
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential_jitter(
                initial=self._base_wait, max=self._max_wait, jitter=1.0
            ),
            reraise=True,
        )

        for attempt in retrying:
            with attempt:
                return self._invoke_once(message_text, sender, group)

        # Tecnicamente inalcançável — `reraise=True` garante propagação.
        raise LLMTransportError("tenacity esgotou tentativas sem exceção registrada")

    def _invoke_once(self, message_text: str, sender: str, group: str) -> dict:
        try:
            raw = self._extract(message_text=message_text, sender=sender, group=group)
        except RuntimeError as e:
            # gemini_api._call_with_retry levanta RuntimeError após esgotar fallbacks.
            raise LLMTransportError(str(e)) from e
        except Exception as e:
            raise LLMTransportError(str(e)) from e

        # gemini_api oscila entre list/dict — normalizamos.
        if isinstance(raw, list):
            raw = raw[0] if raw else {}

        if not isinstance(raw, dict):
            raise LLMDegradedResponse(
                f"Tipo de resposta inesperado: {type(raw).__name__}"
            )

        # ask_gemini_json devolve este shape quando o JSON não parseia.
        if "error" in raw and "raw" in raw:
            raise LLMDegradedResponse(f"JSON parse error: {raw.get('error')}")

        if not raw:
            raise LLMDegradedResponse("Resposta vazia do LLM")

        return raw
