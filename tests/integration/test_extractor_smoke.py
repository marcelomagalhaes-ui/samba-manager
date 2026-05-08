"""
tests/integration/test_extractor_smoke.py
==========================================
Teste de integração (smoke) do ExtractorAgent.

Cobre o fluxo completo: Mensagem → Deal → Sheets → Drive → PDF →
Follow-up (se incompleto) → Notificação interna.

Todos os serviços externos mockados. Banco em memória para isolamento.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.database import Base, Message, Deal, FollowUp


# ─────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────

@pytest.fixture
def engine():
    e = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(e)
    return e


@pytest.fixture
def session(engine):
    S = sessionmaker(bind=engine)
    s = S()
    yield s
    s.close()


@pytest.fixture
def msg_completa(session):
    msg = Message(
        timestamp=datetime(2026, 4, 14, 10, 0),
        sender="Rokane",
        content="50.000 MT Soja Grão FOB Santos USD 490/MT safra 26/27.",
        group_name="Fornecedores Graos",
        has_quote=True,
        commodity="soja", price=490.0, volume=50000.0,
        currency="USD", volume_unit="MT", incoterm="FOB",
    )
    session.add(msg)
    session.commit()
    return msg


@pytest.fixture
def msg_incompleta(session):
    msg = Message(
        timestamp=datetime(2026, 4, 14, 11, 0),
        sender="Contato",
        content="Tenho soja, me chama.",
        group_name="Grupo Misc",
        has_quote=True, commodity=None, price=None, volume=None,
    )
    session.add(msg)
    session.commit()
    return msg


def _agent(session):
    """ExtractorAgent sem __init__, serviços externos mockados."""
    from agents.extractor_agent import ExtractorAgent
    a = ExtractorAgent.__new__(ExtractorAgent)
    a.session = session
    a.sheets_sync = MagicMock()
    a.sheets_sync.append_deal_to_sheet.return_value = True
    a.workspace_enabled = True
    a.drive_service = MagicMock()
    a.drive_service.criar_pasta_negocio.return_value = ("FID", "https://drive/x")
    a.pdf_service = MagicMock()
    a.pdf_service.gerar_ficha_pedido.return_value = True
    return a


# Payloads Gemini
GEM_OK = {
    "has_quote": True, "confidence": 0.95,
    "commodity": "Soja", "direction": "ASK",
    "volume": 50000, "volume_unit": "MT",
    "price": 490.0, "currency": "USD",
    "incoterm": "FOB", "location": "Santos",
}
GEM_INCOMPLETO = {
    "has_quote": True, "confidence": 0.75,
    "commodity": None, "direction": "UNKNOWN",
    "volume": None, "price": None,
    "currency": "USD", "incoterm": None, "location": None,
}
GEM_LOW_CONF = {"has_quote": True, "confidence": 0.4, "commodity": "soja"}
RISK_OK  = {"score": 35, "level": "BAIXO", "factors": [], "recommendation": ""}
RISK_MED = {"score": 60, "level": "MEDIO", "factors": [], "recommendation": ""}


# ─────────────────────────────────────────────────────────────────
# Testes — deal completo
# ─────────────────────────────────────────────────────────────────

@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_deal_criado_no_banco(mock_ex, mock_ri, session, msg_completa):
    created = _agent(session)._process_one_message(msg_completa)
    assert created == 1
    deal = session.query(Deal).filter(Deal.source_message_id == msg_completa.id).first()
    assert deal is not None
    assert deal.commodity == "Soja"
    assert deal.price == 490.0
    assert deal.direcao == "ASK"
    assert deal.status == "ativo"


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_deal_completo_stage_lead_capturado(mock_ex, mock_ri, session, msg_completa):
    _agent(session)._process_one_message(msg_completa)
    deal = session.query(Deal).filter(Deal.source_message_id == msg_completa.id).first()
    assert deal.stage == "Lead Capturado"


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_sheets_sync_chamado(mock_ex, mock_ri, session, msg_completa):
    a = _agent(session)
    a._process_one_message(msg_completa)
    a.sheets_sync.append_deal_to_sheet.assert_called_once()
    payload = a.sheets_sync.append_deal_to_sheet.call_args[0][0]
    assert payload["commodity"] == "Soja"
    assert payload["price"] == 490.0


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_pasta_drive_criada(mock_ex, mock_ri, session, msg_completa):
    a = _agent(session)
    a._process_one_message(msg_completa)
    a.drive_service.criar_pasta_negocio.assert_called_once()


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_pdf_gerado(mock_ex, mock_ri, session, msg_completa):
    a = _agent(session)
    a._process_one_message(msg_completa)
    a.pdf_service.gerar_ficha_pedido.assert_called_once()


@patch("agents.extractor_agent.analyze_deal_risk", return_value={"score": 72, "level": "ALTO", "factors": ["sem LOI"], "recommendation": "Solicitar LOI"})
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_risk_score_salvo(mock_ex, mock_ri, session, msg_completa):
    _agent(session)._process_one_message(msg_completa)
    deal = session.query(Deal).filter(Deal.source_message_id == msg_completa.id).first()
    assert deal.risk_score == 72


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_assignee_soja_eh_leonardo(mock_ex, mock_ri, session, msg_completa):
    _agent(session)._process_one_message(msg_completa)
    deal = session.query(Deal).filter(Deal.source_message_id == msg_completa.id).first()
    assert deal.assignee == "Leonardo"


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_LOW_CONF)
def test_baixa_confianca_ignorada(mock_ex, mock_ri, session, msg_completa):
    created = _agent(session)._process_one_message(msg_completa)
    assert created == 0
    assert session.query(Deal).filter(Deal.source_message_id == msg_completa.id).first() is None


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_OK)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_OK)
def test_idempotencia_sem_duplicar_deal(mock_ex, mock_ri, session, msg_completa):
    """Processar a mesma mensagem duas vezes cria apenas 1 deal."""
    _agent(session)._process_one_message(msg_completa)
    # segunda passagem — deal já existe com source_message_id
    mock_ex.return_value = GEM_OK
    _agent(session)._process_one_message(msg_completa)
    deals = session.query(Deal).filter(Deal.source_message_id == msg_completa.id).all()
    assert len(deals) == 1


# ─────────────────────────────────────────────────────────────────
# Testes — deal incompleto
# ─────────────────────────────────────────────────────────────────

@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_MED)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_INCOMPLETO)
@patch("services.internal_notify.InternalNotifyService.alert_missing_fields",
       return_value={"email": True, "whatsapp": False})
def test_incompleto_vai_para_qualificacao(mock_al, mock_ex, mock_ri, session, msg_incompleta):
    _agent(session)._process_one_message(msg_incompleta)
    deal = session.query(Deal).filter(Deal.source_message_id == msg_incompleta.id).first()
    assert deal is not None
    assert deal.stage == "Qualificação"


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_MED)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_INCOMPLETO)
@patch("services.internal_notify.InternalNotifyService.alert_missing_fields",
       return_value={"email": True, "whatsapp": False})
def test_incompleto_cria_followup_pendente(mock_al, mock_ex, mock_ri, session, msg_incompleta):
    _agent(session)._process_one_message(msg_incompleta)
    fus = session.query(FollowUp).all()
    assert len(fus) >= 1
    assert fus[0].status == "pendente"


@patch("agents.extractor_agent.analyze_deal_risk", return_value=RISK_MED)
@patch("agents.extractor_agent.extract_quote_data", return_value=GEM_INCOMPLETO)
@patch("services.internal_notify.InternalNotifyService.alert_missing_fields",
       return_value={"email": True, "whatsapp": False})
def test_incompleto_notifica_interno(mock_al, mock_ex, mock_ri, session, msg_incompleta):
    _agent(session)._process_one_message(msg_incompleta)
    mock_al.assert_called_once()
    kwargs = mock_al.call_args[1]
    assert len(kwargs.get("missing", [])) > 0
