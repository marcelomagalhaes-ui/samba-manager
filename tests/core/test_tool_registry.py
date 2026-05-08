"""
Testes do ToolRegistry — schemas dual-format e dispatch real ao DB.

Para os tools persistentes (`create_deal`, `update_deal_stage`, `send_followup`)
usamos um SQLite in-memory monkey-patcheando `models.database.get_session`.
"""
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import models.database as db_mod
from core.tool_registry import ToolRegistry, registry
from models.database import Base, Deal, FollowUp


# ----------------------------------------------------------------------------
# Fixture: sessão SQLite em memória, isolada por teste.
# ----------------------------------------------------------------------------

@pytest.fixture
def memory_session(monkeypatch):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    sessions = []

    def _factory(*_args, **_kwargs):
        s = SessionLocal()
        sessions.append(s)
        return s

    monkeypatch.setattr(db_mod, "get_session", _factory)
    yield SessionLocal
    for s in sessions:
        s.close()


# ----------------------------------------------------------------------------
# Schemas
# ----------------------------------------------------------------------------

def test_registry_lists_seeded_tools():
    names = registry.list_names()
    assert "create_deal" in names
    assert "update_deal_stage" in names
    assert "send_followup" in names


def test_to_gemini_declarations_shape():
    decls = registry.to_gemini_declarations()
    assert all({"name", "description", "parameters"} <= d.keys() for d in decls)


def test_to_openai_tools_shape():
    tools = registry.to_openai_tools()
    for t in tools:
        assert t["type"] == "function"
        assert {"name", "description", "parameters"} <= t["function"].keys()


def test_register_rejects_duplicate():
    r = ToolRegistry()
    r.register(name="x", description="d", parameters={"type": "object"})(lambda: None)
    with pytest.raises(ValueError):
        r.register(name="x", description="d", parameters={"type": "object"})(lambda: None)


def test_execute_unknown_raises():
    with pytest.raises(KeyError):
        registry.execute("nope")


# ----------------------------------------------------------------------------
# Handlers reais — persistem no SQLite in-memory
# ----------------------------------------------------------------------------

def test_create_deal_persists(memory_session):
    out = registry.execute(
        "create_deal",
        commodity="SOJA",
        direcao="ASK",
        volume=10000,
        price=420.5,
        currency="USD",
        incoterm="FOB",
        origin="Santos",
    )
    assert "deal_id" in out and out["stage"] == "Lead Capturado"

    s = memory_session()
    try:
        deal = s.query(Deal).filter(Deal.id == out["deal_id"]).one()
        assert deal.commodity == "SOJA"
        assert deal.direcao == "ASK"
        assert deal.price == 420.5
    finally:
        s.close()


def test_update_deal_stage_persists(memory_session):
    create = registry.execute("create_deal", commodity="MILHO", direcao="BID")
    out = registry.execute(
        "update_deal_stage",
        deal_id=create["deal_id"],
        stage="Negociação",
        notes="cliente respondeu",
    )
    assert out == {"deal_id": create["deal_id"], "stage": "Negociação"}

    s = memory_session()
    try:
        deal = s.query(Deal).filter(Deal.id == create["deal_id"]).one()
        assert deal.stage == "Negociação"
        assert "cliente respondeu" in (deal.notes or "")
    finally:
        s.close()


def test_update_deal_stage_unknown_raises(memory_session):
    with pytest.raises(ValueError):
        registry.execute("update_deal_stage", deal_id=99999, stage="Fechado")


def test_send_followup_creates_pending(memory_session):
    out = registry.execute(
        "send_followup",
        target_person="+5511999990001",
        message="cobrar resposta",
        due_in_hours=2,
    )
    assert "followup_id" in out
    s = memory_session()
    try:
        fu = s.query(FollowUp).filter(FollowUp.id == out["followup_id"]).one()
        assert fu.status == "pendente"
        assert fu.target_person == "+5511999990001"
        assert fu.due_at > datetime.utcnow()
    finally:
        s.close()
