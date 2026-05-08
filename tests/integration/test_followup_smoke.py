"""
tests/integration/test_followup_smoke.py
=========================================
Testes de integração (smoke) do FollowUpAgent.

Cobre o ciclo completo:
  - Dispatch de follow-up vencido → envio via WPP → status=enviado
  - Detecção de resposta (response_received=True) → status=respondido → deal avança
  - Escalação de follow-up sem resposta há >3 dias → status=expirado
  - Idempotência: follow-up já enviado não é reenviado
  - Batch limit respeita max_batch
  - dry_run não chama wpp.send
  - check_responses só processa respostas, sem dispatch

Todos os serviços externos mockados (WPP, Gemini). Banco em memória (StaticPool).
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.database import Base, Deal, FollowUp
import models.database as db_mod

# Importa o módulo do agente antecipadamente para que o monkeypatch consiga
# substituir a referência de `get_session` dentro dele.
import agents.followup_agent as fu_mod


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mem_engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def Session(mem_engine):
    return sessionmaker(bind=mem_engine)


@pytest.fixture(autouse=True)
def patch_db(Session, monkeypatch):
    """
    Redireciona get_session para o banco em memória em TODOS os pontos de
    importação relevantes: models.database e agents.followup_agent.
    """
    factory = lambda *a, **kw: Session()
    monkeypatch.setattr(db_mod, "get_session", factory)
    monkeypatch.setattr(fu_mod, "get_session", factory)


@pytest.fixture
def deal(Session):
    sess = Session()
    d = Deal(
        name="SOJA_SMOKE",
        commodity="Soja",
        stage="Lead Capturado",
        assignee="Leonardo",
        status="ativo",
        source_sender="+5511999000001",
        source_group="Grupo Soja",
    )
    sess.add(d)
    sess.commit()
    deal_id = d.id
    sess.close()
    return deal_id


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_followup(Session, deal_id, *, status="pendente", due_offset_hours=-1,
                   sent_offset_days=0, response_received=False):
    """Cria um FollowUp com parâmetros convenientes."""
    sess = Session()
    now = datetime.datetime.utcnow()
    fu = FollowUp(
        deal_id=deal_id,
        target_person="+5511999000001",
        target_group="Grupo Soja",
        message="Olá, ainda temos interesse no negócio?",
        due_at=now + datetime.timedelta(hours=due_offset_hours),
        sent_at=(now - datetime.timedelta(days=sent_offset_days)) if status != "pendente" else None,
        status=status,
        response_received=response_received,
        response_content="Sim, podemos fechar!" if response_received else None,
    )
    sess.add(fu)
    sess.commit()
    fu_id = fu.id
    sess.close()
    return fu_id


def _build_agent():
    """
    Instancia FollowUpAgent com WPP e Gemini mockados.
    Deve ser chamado dentro do contexto dos patches relevantes.
    """
    mock_wpp = MagicMock()
    mock_wpp.send.return_value = {"sid": "WA_TEST_001"}

    with patch("services.whatsapp_api.get_whatsapp_manager", return_value=mock_wpp):
        agent = fu_mod.FollowUpAgent()
        agent.wpp = mock_wpp  # sobrescreve para garantir

    return agent, mock_wpp


# ─────────────────────────────────────────────────────────────────────────────
# Testes
# ─────────────────────────────────────────────────────────────────────────────

def test_followup_vencido_enviado(Session, deal):
    """Follow-up com due_at no passado deve ser enviado (via email com WHATSAPP_OFFLINE=true)."""
    fu_id = _make_followup(Session, deal, status="pendente", due_offset_hours=-2)

    mock_notifier = MagicMock()
    mock_notifier.alert_followup_dispatch.return_value = {"email": True, "whatsapp": False}

    with patch("agents.followup_agent.ask_claude", return_value="Olá! Retorno sobre o negócio?"), \
         patch("agents.followup_agent.WHATSAPP_OFFLINE", True), \
         patch("services.internal_notify.get_notifier", return_value=mock_notifier):
        agent, mock_wpp = _build_agent()
        result = agent.process({"dry_run": False, "max_batch": 10})
        agent.session.close()

    assert result["sent"] >= 1, f"Esperado sent>=1, got {result}"
    assert result["errors"] == 0

    # Com WHATSAPP_OFFLINE=true: email enviado, wpp.send NÃO chamado
    mock_notifier.alert_followup_dispatch.assert_called_once()
    mock_wpp.send.assert_not_called()

    sess = Session()
    fu = sess.query(FollowUp).filter(FollowUp.id == fu_id).first()
    assert fu.status == "enviado"
    assert fu.sent_at is not None
    sess.close()


def test_followup_futuro_nao_enviado(Session, deal):
    """Follow-up com due_at no futuro NÃO deve ser enviado."""
    fu_id = _make_followup(Session, deal, status="pendente", due_offset_hours=+24)

    with patch("agents.followup_agent.ask_claude", return_value="mensagem"):
        agent, mock_wpp = _build_agent()
        result = agent.process({"dry_run": False, "max_batch": 10})
        agent.session.close()

    assert result["sent"] == 0
    mock_wpp.send.assert_not_called()

    sess = Session()
    fu = sess.query(FollowUp).filter(FollowUp.id == fu_id).first()
    assert fu.status == "pendente"
    sess.close()


def test_followup_ja_enviado_nao_reenviado(Session, deal):
    """Follow-up já com status=enviado não entra no batch de dispatch."""
    _make_followup(Session, deal, status="enviado", due_offset_hours=-1, sent_offset_days=1)

    with patch("agents.followup_agent.ask_claude", return_value="mensagem"):
        agent, mock_wpp = _build_agent()
        result = agent.process({"dry_run": False, "max_batch": 10})
        agent.session.close()

    assert result["sent"] == 0
    mock_wpp.send.assert_not_called()


def test_resposta_recebida_avanca_deal(Session, deal):
    """response_received=True → status=respondido + deal avança para Em Negociação."""
    fu_id = _make_followup(
        Session, deal,
        status="enviado", due_offset_hours=-2, sent_offset_days=1,
        response_received=True,
    )

    with patch("agents.followup_agent.ask_claude", return_value="mensagem"):
        agent, _ = _build_agent()
        result = agent.process({"dry_run": False, "max_batch": 10})
        agent.session.close()

    assert result["responses_processed"] >= 1, f"Got {result}"

    sess = Session()
    fu = sess.query(FollowUp).filter(FollowUp.id == fu_id).first()
    assert fu.status == "respondido"

    updated_deal = sess.query(Deal).filter(Deal.id == deal).first()
    assert updated_deal.stage == "Em Negociação"
    sess.close()


def test_followup_expirado_escalado(Session, deal):
    """Follow-up enviado há >3 dias sem resposta deve ser escalado e marcado expirado."""
    fu_id = _make_followup(
        Session, deal,
        status="enviado", due_offset_hours=-80, sent_offset_days=4,
        response_received=False,
    )

    mock_wpp = MagicMock()
    mock_wpp.send.return_value = {"sid": "WA_ESC_001"}

    with patch("agents.followup_agent.ask_claude", return_value="Cobrança"), \
         patch("services.whatsapp_api.get_whatsapp_manager", return_value=mock_wpp), \
         patch.dict("os.environ", {
             "SOCIO_1_NAME": "Leonardo", "SOCIO_1_PHONE": "+5511999999001",
             "SOCIO_2_NAME": "Nivio",    "SOCIO_2_PHONE": "+5511999999002",
             "SOCIO_3_NAME": "Marcelo",  "SOCIO_3_PHONE": "+5511999999003",
         }):
        agent = fu_mod.FollowUpAgent()
        agent.wpp = mock_wpp
        result = agent.process({"dry_run": False, "max_batch": 10})
        agent.session.close()

    assert result["escalated"] >= 1, f"Got {result}"

    sess = Session()
    fu = sess.query(FollowUp).filter(FollowUp.id == fu_id).first()
    assert fu.status == "expirado"
    sess.close()


def test_batch_limit_respeitado(Session, deal):
    """max_batch=2 com 5 follow-ups vencidos → só 2 enviados (2 emails)."""
    for _ in range(5):
        _make_followup(Session, deal, status="pendente", due_offset_hours=-1)

    mock_notifier = MagicMock()
    mock_notifier.alert_followup_dispatch.return_value = {"email": True, "whatsapp": False}

    with patch("agents.followup_agent.ask_claude", return_value="mensagem"), \
         patch("agents.followup_agent.WHATSAPP_OFFLINE", True), \
         patch("services.internal_notify.get_notifier", return_value=mock_notifier):
        agent, mock_wpp = _build_agent()
        result = agent.process({"dry_run": False, "max_batch": 2})
        agent.session.close()

    assert result["sent"] == 2, f"Got {result}"
    assert mock_notifier.alert_followup_dispatch.call_count == 2
    mock_wpp.send.assert_not_called()


def test_followup_wpp_quando_online(Session, deal):
    """Quando WHATSAPP_OFFLINE=False, usa wpp.send (não email)."""
    _make_followup(Session, deal, status="pendente", due_offset_hours=-1)

    with patch("agents.followup_agent.ask_claude", return_value="mensagem"), \
         patch("agents.followup_agent.WHATSAPP_OFFLINE", False):
        agent, mock_wpp = _build_agent()
        result = agent.process({"dry_run": False, "max_batch": 10})
        agent.session.close()

    assert result["sent"] >= 1
    mock_wpp.send.assert_called_once()


def test_dry_run_nao_envia(Session, deal):
    """dry_run=True: registra como enviado no banco mas NÃO chama wpp.send."""
    _make_followup(Session, deal, status="pendente", due_offset_hours=-1)

    with patch("agents.followup_agent.ask_claude", return_value="mensagem"):
        agent, mock_wpp = _build_agent()
        result = agent.process({"dry_run": True, "max_batch": 10})
        agent.session.close()

    assert result["errors"] == 0
    mock_wpp.send.assert_not_called()


def test_check_responses_only(Session, deal):
    """check_responses=True só processa respostas, não faz dispatch de pendentes."""
    _make_followup(Session, deal, status="pendente", due_offset_hours=-1)
    _make_followup(
        Session, deal,
        status="enviado", due_offset_hours=-2, sent_offset_days=1,
        response_received=True,
    )

    with patch("agents.followup_agent.ask_claude", return_value="mensagem"):
        agent, mock_wpp = _build_agent()
        result = agent.process({"check_responses": True})
        agent.session.close()

    # Deve processar a resposta
    assert result["responses_processed"] >= 1, f"Got {result}"
    # NÃO deve despachar o follow-up pendente
    mock_wpp.send.assert_not_called()
