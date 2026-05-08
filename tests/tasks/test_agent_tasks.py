"""
Testes das Celery tasks — execução eager (sem broker), com agentes mockados.

Verificamos:
  - LLMUnavailable NÃO é retentada (regra inviolável).
  - Sucesso devolve o resultado do agente.
  - Workflow `task_process_inbound_message` despacha um `chain` válido.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from core.celery_app import celery_app
from sync.exceptions import LLMUnavailable
from tasks.agent_tasks import (
    task_dispatch_followups,
    task_extract_message,
    task_process_followup_response,
    task_process_inbound_message,
    task_sync_spreadsheet_to_drive,
)


@pytest.fixture(autouse=True)
def eager_celery():
    """Roda tasks sincronamente no processo de teste — sem Redis."""
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    yield
    celery_app.conf.task_always_eager = False
    celery_app.conf.task_eager_propagates = False


# ----------------------------------------------------------------------------
# task_extract_message
# ----------------------------------------------------------------------------

def test_task_extract_message_success():
    fake_agent = MagicMock()
    fake_agent.process_single_message.return_value = {"deals_created": 1, "msg_id": 42}

    with patch("agents.extractor_agent.ExtractorAgent", return_value=fake_agent):
        out = task_extract_message.apply(args=(42,)).get()

    assert out == {"deals_created": 1, "msg_id": 42}
    fake_agent.process_single_message.assert_called_once_with(42)


def test_task_extract_message_llm_unavailable_does_not_retry():
    fake_agent = MagicMock()
    fake_agent.process_single_message.side_effect = LLMUnavailable("quota")

    with patch("agents.extractor_agent.ExtractorAgent", return_value=fake_agent):
        out = task_extract_message.apply(args=(7,)).get()

    assert out["skipped"] == "llm_unavailable"
    assert out["msg_id"] == 7
    # Garantia: foi chamado UMA vez (sem retry).
    assert fake_agent.process_single_message.call_count == 1


# ----------------------------------------------------------------------------
# task_sync_spreadsheet_to_drive
# ----------------------------------------------------------------------------

def test_task_sync_success():
    fake_agent = MagicMock()
    with patch("agents.spreadsheet_sync_agent.SpreadsheetSyncAgent", return_value=fake_agent):
        out = task_sync_spreadsheet_to_drive.apply().get()
    assert out == {"status": "ok"}
    fake_agent.sincronizar_planilha_para_drive.assert_called_once()


def test_task_sync_llm_unavailable_degrades():
    fake_agent = MagicMock()
    fake_agent.sincronizar_planilha_para_drive.side_effect = LLMUnavailable("breaker")
    with patch("agents.spreadsheet_sync_agent.SpreadsheetSyncAgent", return_value=fake_agent):
        out = task_sync_spreadsheet_to_drive.apply().get()
    assert out["status"] == "degraded"
    assert "breaker" in out["reason"]


# ----------------------------------------------------------------------------
# Workflow chain
# ----------------------------------------------------------------------------

def test_process_inbound_dispatches_chain():
    """Em modo eager, o `apply_async` do chain executa as duas tasks downstream."""
    extractor = MagicMock()
    extractor.process_single_message.return_value = {"deals_created": 1, "msg_id": 9}
    sync = MagicMock()

    with patch("agents.extractor_agent.ExtractorAgent", return_value=extractor), \
         patch("agents.spreadsheet_sync_agent.SpreadsheetSyncAgent", return_value=sync):
        out = task_process_inbound_message.apply(args=(9,)).get()

    assert out["msg_id"] == 9
    assert "workflow_id" in out
    extractor.process_single_message.assert_called_once_with(9)
    sync.sincronizar_planilha_para_drive.assert_called_once()


# ----------------------------------------------------------------------------
# task_dispatch_followups
# ----------------------------------------------------------------------------

def test_task_dispatch_followups_success():
    """Chama FollowUpAgent.process e devolve seu resultado."""
    fake_agent = MagicMock()
    fake_agent.process.return_value = {
        "sent": 3, "skipped": 0, "errors": 0,
        "responses_processed": 1, "escalated": 0,
    }

    with patch("agents.followup_agent.FollowUpAgent", return_value=fake_agent):
        out = task_dispatch_followups.apply(args=()).get()

    fake_agent.process.assert_called_once_with({"dry_run": False, "max_batch": 5})
    fake_agent.session.close.assert_called_once()
    assert out["sent"] == 3
    assert out["responses_processed"] == 1


def test_task_dispatch_followups_dry_run():
    """dry_run=True é passado diretamente ao agente."""
    fake_agent = MagicMock()
    fake_agent.process.return_value = {
        "sent": 0, "skipped": 2, "errors": 0,
        "responses_processed": 0, "escalated": 0,
    }

    with patch("agents.followup_agent.FollowUpAgent", return_value=fake_agent):
        out = task_dispatch_followups.apply(kwargs={"dry_run": True, "max_batch": 5}).get()

    fake_agent.process.assert_called_once_with({"dry_run": True, "max_batch": 5})
    assert out["sent"] == 0


# ----------------------------------------------------------------------------
# task_process_followup_response
# ----------------------------------------------------------------------------

def test_task_process_followup_response_marks_responded():
    """Marca FollowUp como respondido e notifica o assignee."""
    from unittest.mock import MagicMock, patch

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from models.database import Base, Deal, FollowUp
    import models.database as db_mod
    import datetime

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    # Popula deal + followup
    sess = Session()
    deal = Deal(
        name="SOJA_TEST", commodity="Soja", stage="Lead Capturado",
        assignee="Leonardo", status="ativo",
    )
    sess.add(deal)
    sess.flush()
    deal_id = deal.id  # captura antes do close
    fu = FollowUp(
        deal_id=deal_id,
        target_person="+5511111110001",
        status="enviado",
        response_received=True,
        response_content="Sim, podemos fechar.",
        due_at=datetime.datetime.utcnow(),
    )
    sess.add(fu)
    sess.commit()
    fu_id = fu.id
    sess.close()

    mock_notifier = MagicMock()
    mock_notifier.alert_followup_responded.return_value = {"email": True, "whatsapp": False}

    with patch.object(db_mod, "get_session", lambda *a, **kw: Session()), \
         patch("services.internal_notify.get_notifier", return_value=mock_notifier):
        out = task_process_followup_response.apply(args=(fu_id,)).get()

    assert out["status"] == "ok"
    assert out["followup_id"] == fu_id

    # Verifica que o deal avançou de stage
    verif = Session()
    updated_deal = verif.query(Deal).filter(Deal.id == deal_id).first()
    assert updated_deal.stage == "Em Negociação"
    # FollowUp marcado como respondido
    updated_fu = verif.query(FollowUp).filter(FollowUp.id == fu_id).first()
    assert updated_fu.status == "respondido"
    verif.close()

    mock_notifier.alert_followup_responded.assert_called_once()


def test_task_process_followup_response_not_found():
    """followup_id inexistente devolve status=not_found sem explodir."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool
    from models.database import Base
    import models.database as db_mod

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with patch.object(db_mod, "get_session", lambda *a, **kw: Session()):
        out = task_process_followup_response.apply(args=(9999,)).get()

    assert out["status"] == "not_found"
    assert out["followup_id"] == 9999
