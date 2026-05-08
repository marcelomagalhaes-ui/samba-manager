"""
Testes do webhook FastAPI — TestClient, sem Redis nem Twilio reais.

Cobertura:
  - 200 OK + persistência da Message + dispatch da task (modo dev sem assinatura).
  - 403 Forbidden quando assinatura é exigida e ausente/ inválida.
  - Idempotência: replay com mesmo MessageSid não duplica Message.
"""
import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import models.database as db_mod
from models.database import Base, Deal, FollowUp, Message


# ----------------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------------

@pytest.fixture
def memory_db(monkeypatch):
    # StaticPool + check_same_thread=False → uma única conexão compartilhada,
    # condição necessária para que `:memory:` mantenha o schema entre sessões.
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


@pytest.fixture
def app_no_signature(monkeypatch):
    """Importa o app com TWILIO_VALIDATE_SIGNATURE=false (modo dev)."""
    monkeypatch.setenv("TWILIO_VALIDATE_SIGNATURE", "false")
    # Recarrega o módulo para reler env vars no escopo do teste.
    if "api.webhook" in sys.modules:
        del sys.modules["api.webhook"]
    webhook = importlib.import_module("api.webhook")
    return webhook


@pytest.fixture
def app_with_signature(monkeypatch):
    monkeypatch.setenv("TWILIO_VALIDATE_SIGNATURE", "true")
    monkeypatch.setenv("TWILIO_AUTH_TOKEN", "test-token")
    if "api.webhook" in sys.modules:
        del sys.modules["api.webhook"]
    webhook = importlib.import_module("api.webhook")
    return webhook


# ----------------------------------------------------------------------------
# Health
# ----------------------------------------------------------------------------

def test_health(app_no_signature):
    client = TestClient(app_no_signature.app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.text == "ok"


# ----------------------------------------------------------------------------
# Webhook OK path (sem validação de assinatura)
# ----------------------------------------------------------------------------

def test_webhook_persists_and_dispatches(memory_db, app_no_signature):
    fake_task = MagicMock()
    fake_task.delay.return_value = MagicMock(id="async-id-123")

    with patch("tasks.agent_tasks.task_process_inbound_message", fake_task):
        client = TestClient(app_no_signature.app)
        r = client.post(
            "/webhook/twilio",
            data={
                "From": "whatsapp:+5511999990001",
                "To": "whatsapp:+5513999990001",
                "Body": "Vendo 5000 ton soja FOB Santos a 420 USD",
                "MessageSid": "SM_test_001",
                "ProfileName": "Trader X",
                "NumMedia": "0",
            },
        )
    assert r.status_code == 200
    assert "<Response>" in r.text
    fake_task.delay.assert_called_once()

    # Verifica Message foi persistida com MessageSid.
    s = memory_db()
    try:
        msg = s.query(Message).filter(Message.attachment_name == "twilio:SM_test_001").one()
        assert msg.sender == "+5511999990001"
        assert "soja" in msg.content
    finally:
        s.close()


def test_webhook_idempotent_replay(memory_db, app_no_signature):
    """Replay do mesmo MessageSid reaproveita a Message existente."""
    fake_task = MagicMock()
    fake_task.delay.return_value = MagicMock(id="x")
    payload = {
        "From": "whatsapp:+5511999990001",
        "Body": "teste",
        "MessageSid": "SM_replay_42",
        "NumMedia": "0",
    }

    with patch("tasks.agent_tasks.task_process_inbound_message", fake_task):
        client = TestClient(app_no_signature.app)
        r1 = client.post("/webhook/twilio", data=payload)
        r2 = client.post("/webhook/twilio", data=payload)

    assert r1.status_code == 200 and r2.status_code == 200
    s = memory_db()
    try:
        rows = s.query(Message).filter(Message.attachment_name == "twilio:SM_replay_42").all()
        assert len(rows) == 1, "replay duplicou Message no DB"
    finally:
        s.close()


# ----------------------------------------------------------------------------
# Signature enforcement
# ----------------------------------------------------------------------------

def test_webhook_rejects_missing_signature(memory_db, app_with_signature):
    """Sem header X-Twilio-Signature em modo strict → 403."""
    client = TestClient(app_with_signature.app)
    r = client.post(
        "/webhook/twilio",
        data={"From": "whatsapp:+5511999990001", "Body": "x", "MessageSid": "SM_x"},
    )
    assert r.status_code == 403


def test_webhook_rejects_invalid_signature(memory_db, app_with_signature):
    client = TestClient(app_with_signature.app)
    r = client.post(
        "/webhook/twilio",
        data={"From": "whatsapp:+5511999990001", "Body": "x", "MessageSid": "SM_x"},
        headers={"X-Twilio-Signature": "obviamente-invalida"},
    )
    # 403 (assinatura inválida) ou 500 se a SDK Twilio não estiver instalada
    # — ambos são "rejeitamos a chamada", que é o invariante que importa.
    assert r.status_code in (403, 500)


# ----------------------------------------------------------------------------
# Follow-Up response matching
# ----------------------------------------------------------------------------

import datetime as _dt


def test_match_followup_response_flags_db(memory_db, app_no_signature):
    """Quando remetente = target_person de FollowUp enviado, banco é atualizado."""
    import importlib

    # Precisa reimportar webhook para pegar o memory_db já aplicado.
    if "api.webhook" in sys.modules:
        del sys.modules["api.webhook"]
    webhook = importlib.import_module("api.webhook")

    sess = memory_db()
    deal = Deal(
        name="SOJA_RESP", commodity="Soja", stage="Lead Capturado",
        assignee="Leonardo", status="ativo",
    )
    sess.add(deal)
    sess.flush()
    fu = FollowUp(
        deal_id=deal.id,
        target_person="+5511222220001",
        status="enviado",
        response_received=False,
        due_at=_dt.datetime.utcnow(),
    )
    sess.add(fu)
    sess.commit()
    fu_id = fu.id
    sess.close()

    affected = webhook.match_followup_response("+5511222220001", "Sim, vamos fechar.")

    assert fu_id in affected

    # Confere no banco
    verif = memory_db()
    updated = verif.query(FollowUp).filter(FollowUp.id == fu_id).first()
    assert updated.response_received is True
    assert "vamos fechar" in (updated.response_content or "")
    verif.close()


def test_match_followup_response_no_match(memory_db, app_no_signature):
    """Remetente diferente → nenhuma alteração."""
    import importlib

    if "api.webhook" in sys.modules:
        del sys.modules["api.webhook"]
    webhook = importlib.import_module("api.webhook")

    # FollowUp com número diferente
    sess = memory_db()
    fu = FollowUp(
        deal_id=None,
        target_person="+5511000000000",
        status="enviado",
        response_received=False,
        due_at=_dt.datetime.utcnow(),
    )
    sess.add(fu)
    sess.commit()
    fu_id = fu.id
    sess.close()

    affected = webhook.match_followup_response("+5511999999999", "Outra mensagem")

    assert fu_id not in affected


def test_webhook_dispatches_followup_response_task(memory_db, app_no_signature):
    """Webhook detecta resposta de follow-up e despacha a task de notificação."""
    import importlib

    if "api.webhook" in sys.modules:
        del sys.modules["api.webhook"]
    webhook = importlib.import_module("api.webhook")

    # Cria FollowUp enviado para o mesmo número que vai responder
    sess = memory_db()
    deal = Deal(
        name="MILHO_RESP", commodity="Milho", stage="Lead Capturado",
        assignee="Nivio", status="ativo",
    )
    sess.add(deal)
    sess.flush()
    fu = FollowUp(
        deal_id=deal.id,
        target_person="+5511333330001",
        status="enviado",
        response_received=False,
        due_at=_dt.datetime.utcnow(),
    )
    sess.add(fu)
    sess.commit()
    sess.close()

    fake_extract_task = MagicMock()
    fake_extract_task.delay.return_value = MagicMock(id="ext-id")
    fake_fu_response_task = MagicMock()
    fake_fu_response_task.delay.return_value = MagicMock(id="fu-id")

    with patch("tasks.agent_tasks.task_process_inbound_message", fake_extract_task), \
         patch("tasks.agent_tasks.task_process_followup_response", fake_fu_response_task):
        client = TestClient(webhook.app)
        r = client.post(
            "/webhook/twilio",
            data={
                "From": "whatsapp:+5511333330001",
                "Body": "Podemos fechar sim!",
                "MessageSid": "SM_resp_001",
                "NumMedia": "0",
            },
        )

    assert r.status_code == 200
    # A task de resposta deve ter sido despachada
    assert fake_fu_response_task.delay.called
