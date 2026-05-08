"""
Testes do LLMGateway + CircuitBreaker.
Não tocam no Gemini real — injetamos um `extract_fn` fake.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from sync import (
    CircuitBreaker,
    CircuitBreakerOpen,
    LLMDegradedResponse,
    LLMGateway,
    LLMTransportError,
    LLMUnavailable,
)


# ---------------------------------------------------------------------------
# Helpers — clock congelado e contador de chamadas
# ---------------------------------------------------------------------------

class FakeClock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, delta: float) -> None:
        self.t += delta


def _gateway(
    extract_fn,
    breaker=None,
    max_attempts: int = 2,
    clock=None,
) -> LLMGateway:
    return LLMGateway(
        extract_fn=extract_fn,
        breaker=breaker or CircuitBreaker(failure_threshold=3, cooldown_seconds=60),
        max_attempts=max_attempts,
        base_wait=0.0,  # testes não esperam
        max_wait=0.0,
        clock=clock or FakeClock(),
    )


# ---------------------------------------------------------------------------
# Caminho feliz
# ---------------------------------------------------------------------------

def test_happy_path_returns_dict():
    def fake(**kw):
        return {"commodity": "SOJA", "price": 1200}

    gw = _gateway(fake)
    result = gw.extract_quote(message_text="x")
    assert result["commodity"] == "SOJA"
    assert gw.breaker.consecutive_failures == 0


def test_normalizes_list_response():
    def fake(**kw):
        return [{"commodity": "MILHO"}]

    gw = _gateway(fake)
    assert gw.extract_quote(message_text="x")["commodity"] == "MILHO"


def test_empty_list_raises_degraded():
    def fake(**kw):
        return []

    gw = _gateway(fake)
    with pytest.raises(LLMDegradedResponse):
        gw.extract_quote(message_text="x")


# ---------------------------------------------------------------------------
# Respostas degradadas — NÃO retentam (retry não vai curar JSON ruim)
# ---------------------------------------------------------------------------

def test_json_parse_error_shape_becomes_degraded():
    calls = {"n": 0}

    def fake(**kw):
        calls["n"] += 1
        return {"error": "JSON parse error", "raw": "lixo"}

    gw = _gateway(fake, max_attempts=3)
    with pytest.raises(LLMDegradedResponse):
        gw.extract_quote(message_text="x")
    assert calls["n"] == 1, "não deve retentar em resposta degradada"


def test_non_dict_response_becomes_degraded():
    def fake(**kw):
        return "uma string qualquer"

    gw = _gateway(fake)
    with pytest.raises(LLMDegradedResponse):
        gw.extract_quote(message_text="x")


def test_empty_dict_becomes_degraded():
    def fake(**kw):
        return {}

    gw = _gateway(fake)
    with pytest.raises(LLMDegradedResponse):
        gw.extract_quote(message_text="x")


# ---------------------------------------------------------------------------
# Transport errors — retentam via tenacity
# ---------------------------------------------------------------------------

def test_transient_failure_is_retried_then_succeeds():
    calls = {"n": 0}

    def fake(**kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("429 quota exceeded")
        return {"commodity": "CAFÉ"}

    gw = _gateway(fake, max_attempts=2)
    result = gw.extract_quote(message_text="x")
    assert result["commodity"] == "CAFÉ"
    assert calls["n"] == 2


def test_exhausted_retries_raise_transport_error():
    def fake(**kw):
        raise RuntimeError("503 overloaded")

    gw = _gateway(fake, max_attempts=2)
    with pytest.raises(LLMTransportError):
        gw.extract_quote(message_text="x")


def test_unknown_exception_wrapped_as_transport_error():
    def fake(**kw):
        raise ValueError("surpresa")

    gw = _gateway(fake, max_attempts=1)
    with pytest.raises(LLMTransportError):
        gw.extract_quote(message_text="x")


# ---------------------------------------------------------------------------
# Circuit Breaker — contrato de estados
# ---------------------------------------------------------------------------

def test_breaker_opens_after_consecutive_failures():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=2, cooldown_seconds=60)

    def fake(**kw):
        raise RuntimeError("503")

    gw = _gateway(fake, breaker=breaker, max_attempts=1, clock=clock)

    with pytest.raises(LLMTransportError):
        gw.extract_quote(message_text="x")
    assert breaker.is_open(clock()) is False
    assert breaker.consecutive_failures == 1

    with pytest.raises(LLMTransportError):
        gw.extract_quote(message_text="x")
    assert breaker.is_open(clock()) is True


def test_open_breaker_fails_fast_without_calling_api():
    calls = {"n": 0}
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=60)
    breaker.opened_at = clock()  # força estado OPEN

    def fake(**kw):
        calls["n"] += 1
        return {"ok": True}

    gw = _gateway(fake, breaker=breaker, clock=clock)
    with pytest.raises(CircuitBreakerOpen):
        gw.extract_quote(message_text="x")
    assert calls["n"] == 0, "API não pode ser chamada com breaker aberto"


def test_breaker_transitions_to_half_open_after_cooldown():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=60)

    def fake_fail(**kw):
        raise RuntimeError("503")

    gw = _gateway(fake_fail, breaker=breaker, max_attempts=1, clock=clock)
    with pytest.raises(LLMUnavailable):
        gw.extract_quote(message_text="x")
    assert breaker.is_open(clock()) is True

    # Pula o cooldown — breaker entra em HALF_OPEN (não bloqueia próxima tentativa)
    clock.advance(61)
    assert breaker.is_open(clock()) is False


def test_half_open_success_closes_breaker():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, cooldown_seconds=60)
    state = {"should_fail": True}

    def fake(**kw):
        if state["should_fail"]:
            raise RuntimeError("503")
        return {"commodity": "MILHO"}

    gw = _gateway(fake, breaker=breaker, max_attempts=1, clock=clock)
    with pytest.raises(LLMUnavailable):
        gw.extract_quote(message_text="x")
    assert breaker.is_open(clock()) is True

    clock.advance(61)
    state["should_fail"] = False
    result = gw.extract_quote(message_text="x")
    assert result["commodity"] == "MILHO"
    assert breaker.opened_at is None
    assert breaker.consecutive_failures == 0


def test_success_resets_failure_counter_even_before_opening():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, cooldown_seconds=60)
    state = {"next_should_fail": True}

    def fake(**kw):
        if state["next_should_fail"]:
            state["next_should_fail"] = False
            raise RuntimeError("503")
        return {"ok": True}

    gw = _gateway(fake, breaker=breaker, max_attempts=1, clock=clock)

    with pytest.raises(LLMUnavailable):
        gw.extract_quote(message_text="x")
    assert breaker.consecutive_failures == 1

    result = gw.extract_quote(message_text="x")
    assert result == {"ok": True}
    assert breaker.consecutive_failures == 0


def test_degraded_response_also_counts_toward_breaker():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=2, cooldown_seconds=60)

    def fake(**kw):
        return {"error": "JSON parse error", "raw": "..."}

    gw = _gateway(fake, breaker=breaker, max_attempts=1, clock=clock)
    for _ in range(2):
        with pytest.raises(LLMDegradedResponse):
            gw.extract_quote(message_text="x")
    assert breaker.is_open(clock()) is True


# ---------------------------------------------------------------------------
# Hierarquia de exceções — orquestrador usa um único catch
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "exc_class",
    [LLMTransportError, LLMDegradedResponse, CircuitBreakerOpen],
)
def test_all_failure_modes_inherit_from_llm_unavailable(exc_class):
    assert issubclass(exc_class, LLMUnavailable)
