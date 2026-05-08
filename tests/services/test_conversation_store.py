"""
Testes do conversation_store — persistencia + traducao DB<->Gemini.
"""
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import models.database as db_mod
from models.database import Base


# ----------------------------------------------------------------------------
# Fixture: DB in-memory compartilhado (StaticPool).
# ----------------------------------------------------------------------------

@pytest.fixture
def memory_db(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    def _factory(*_a, **_kw):
        return SessionLocal()

    monkeypatch.setattr(db_mod, "get_session", _factory)
    return SessionLocal


# ----------------------------------------------------------------------------
# append_turn + load_session
# ----------------------------------------------------------------------------

def test_append_and_load_roundtrip(memory_db):
    from services.conversation_store import append_turn, load_session

    append_turn("S1", "user", "Oi, quanto esta a soja?")
    append_turn("S1", "assistant", "CBOT 420 USD/MT hoje.")

    rows = load_session("S1")
    assert [r["role"] for r in rows] == ["user", "assistant"]
    assert rows[0]["content"] == "Oi, quanto esta a soja?"
    assert rows[1]["content"] == "CBOT 420 USD/MT hoje."


def test_append_role_invalido_levanta(memory_db):
    from services.conversation_store import append_turn
    with pytest.raises(ValueError):
        append_turn("S1", "robo", "ops")


def test_load_session_isola_por_id(memory_db):
    from services.conversation_store import append_turn, load_session
    append_turn("A", "user", "msg-A")
    append_turn("B", "user", "msg-B")
    assert [r["content"] for r in load_session("A")] == ["msg-A"]
    assert [r["content"] for r in load_session("B")] == ["msg-B"]


def test_append_turn_com_tool_calls(memory_db):
    from services.conversation_store import append_turn, load_session
    append_turn(
        "S2", "assistant",
        content="Vou criar o deal.",
        tool_calls=[{"name": "create_deal", "args": {"commodity": "SOJA"}}],
    )
    rows = load_session("S2")
    assert rows[0]["tool_calls"][0]["name"] == "create_deal"


# ----------------------------------------------------------------------------
# db_history_to_gemini
# ----------------------------------------------------------------------------

def test_db_to_gemini_user_virou_text():
    from services.conversation_store import db_history_to_gemini
    out = db_history_to_gemini([{"role": "user", "content": "oi", "tool_calls": None}])
    assert out == [{"role": "user", "parts": [{"text": "oi"}]}]


def test_db_to_gemini_assistant_com_tool_call():
    from services.conversation_store import db_history_to_gemini
    out = db_history_to_gemini([{
        "role": "assistant",
        "content": "Criando deal...",
        "tool_calls": [{"name": "create_deal", "args": {"commodity": "SOJA"}}],
    }])
    assert out[0]["role"] == "model"
    parts = out[0]["parts"]
    assert any(p.get("function_call", {}).get("name") == "create_deal" for p in parts)
    assert any(p.get("text") == "Criando deal..." for p in parts)


def test_db_to_gemini_tool_virou_function_response():
    from services.conversation_store import db_history_to_gemini
    out = db_history_to_gemini([{
        "role": "tool",
        "content": None,
        "tool_calls": [{"name": "create_deal", "result": {"deal_id": 42}}],
    }])
    assert out[0]["role"] == "user"
    fr = out[0]["parts"][0]["function_response"]
    assert fr == {"name": "create_deal", "response": {"deal_id": 42}}


def test_db_to_gemini_ignora_system():
    from services.conversation_store import db_history_to_gemini
    out = db_history_to_gemini([{"role": "system", "content": "ignore-me", "tool_calls": None}])
    assert out == []


# ----------------------------------------------------------------------------
# persist_assistant_turn (wrapper conveniente)
# ----------------------------------------------------------------------------

def test_persist_assistant_turn_grava_assistant_e_tools(memory_db):
    from services.conversation_store import persist_assistant_turn, load_session

    persist_assistant_turn(
        session_id="S3",
        text="Criei o deal 42.",
        tool_calls_trace=[
            {"name": "create_deal", "args": {"commodity": "SOJA"}, "result": {"deal_id": 42}},
        ],
    )
    rows = load_session("S3")
    roles = [r["role"] for r in rows]
    assert roles == ["assistant", "tool"]
    assert rows[0]["content"] == "Criei o deal 42."
    assert rows[1]["tool_calls"][0]["result"] == {"deal_id": 42}
