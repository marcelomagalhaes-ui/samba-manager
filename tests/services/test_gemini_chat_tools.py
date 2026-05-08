"""
Testes do `chat_with_tools` — mock do cliente Gemini para validar o loop
user -> tool_call -> tool_response -> texto final.

NAO chamamos a API Gemini real — injetamos um fake client cujos
`generate_content` retornam respostas pre-definidas.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))


# ----------------------------------------------------------------------------
# Helpers para construir um "candidate" estilo Gemini
# ----------------------------------------------------------------------------

def _make_text_response(text: str):
    """Simula o `response` retornado por genai Client quando ha so texto."""
    resp = MagicMock()
    resp.text = text
    part = MagicMock()
    part.function_call = None
    part.text = text
    candidate = MagicMock()
    candidate.content.parts = [part]
    resp.candidates = [candidate]
    return resp


def _make_tool_call_response(name: str, args: dict):
    resp = MagicMock()
    resp.text = ""
    part = MagicMock()
    fc = MagicMock()
    fc.name = name
    fc.args = args
    part.function_call = fc
    part.text = None
    candidate = MagicMock()
    candidate.content.parts = [part]
    resp.candidates = [candidate]
    return resp


# ----------------------------------------------------------------------------
# Testes
# ----------------------------------------------------------------------------

def test_chat_sem_tools_retorna_texto_direto(monkeypatch):
    from services import gemini_api

    monkeypatch.setattr(gemini_api, "_RATE_SLEEP", 0.0)
    fake_client = MagicMock()
    fake_client.models.generate_content.return_value = _make_text_response("Ola!")
    monkeypatch.setattr(gemini_api, "_get_client", lambda: fake_client)

    out = gemini_api.chat_with_tools(
        user_message="Oi",
        history=[],
        tool_declarations=None,
        tool_executor=None,
    )
    assert out["text"] == "Ola!"
    assert out["tool_calls"] == []


def test_chat_executa_tool_e_devolve_resposta_final(monkeypatch):
    from services import gemini_api

    monkeypatch.setattr(gemini_api, "_RATE_SLEEP", 0.0)

    # 1a chamada: LLM pede tool; 2a chamada: LLM responde texto final.
    fake_client = MagicMock()
    fake_client.models.generate_content.side_effect = [
        _make_tool_call_response("create_deal", {"commodity": "SOJA", "direcao": "ASK"}),
        _make_text_response("Deal criado: ID 42."),
    ]
    monkeypatch.setattr(gemini_api, "_get_client", lambda: fake_client)

    executed = []
    def _exec(name, args):
        executed.append((name, args))
        return {"deal_id": 42, "name": "auto", "stage": "Lead Capturado"}

    out = gemini_api.chat_with_tools(
        user_message="cria um deal de venda de soja",
        history=[],
        tool_declarations=[{"name": "create_deal", "parameters": {"type": "object"}}],
        tool_executor=_exec,
    )

    assert out["text"] == "Deal criado: ID 42."
    assert len(out["tool_calls"]) == 1
    assert out["tool_calls"][0]["name"] == "create_deal"
    assert out["tool_calls"][0]["result"]["deal_id"] == 42
    assert executed == [("create_deal", {"commodity": "SOJA", "direcao": "ASK"})]


def test_chat_exige_executor_quando_ha_tools(monkeypatch):
    from services import gemini_api
    with pytest.raises(ValueError):
        gemini_api.chat_with_tools(
            user_message="oi",
            tool_declarations=[{"name": "x", "parameters": {"type": "object"}}],
            tool_executor=None,
        )


def test_chat_para_loop_em_max_rounds(monkeypatch):
    """Se o LLM so devolve tool_calls, para em max_tool_rounds sem travar."""
    from services import gemini_api

    monkeypatch.setattr(gemini_api, "_RATE_SLEEP", 0.0)
    fake_client = MagicMock()
    # Sempre devolve tool call — nunca texto. O loop deve parar em max_tool_rounds.
    fake_client.models.generate_content.return_value = _make_tool_call_response(
        "create_deal", {"commodity": "X"},
    )
    monkeypatch.setattr(gemini_api, "_get_client", lambda: fake_client)

    out = gemini_api.chat_with_tools(
        user_message="loop",
        tool_declarations=[{"name": "create_deal", "parameters": {"type": "object"}}],
        tool_executor=lambda n, a: {"ok": True},
        max_tool_rounds=2,
    )
    # Texto final e um aviso, mas a funcao retornou sem travar.
    assert "Limite" in out["text"]
    assert len(out["tool_calls"]) == 2  # 2 rounds, 1 tool por round
