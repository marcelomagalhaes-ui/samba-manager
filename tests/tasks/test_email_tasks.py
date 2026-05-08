"""
tests/tasks/test_email_tasks.py
================================
Testes unitários das tasks de email (DB mockado, notifier mockado).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from tasks.email_tasks import (
    _pipeline_por_stage,
    _deals_criados_no_periodo,
    _deals_em_qualificacao,
    _followups_vencidos,
    _deals_stale_sem_followup,
    task_morning_brief,
    task_eod_closing,
    task_intraday_alert,
)


# ──────────────────────────────────────────────────────────────────
# Helpers de query (testados com sessão mock)
# ──────────────────────────────────────────────────────────────────

def _make_session(rows=None, scalar=0):
    session = MagicMock()
    result  = MagicMock()
    result.fetchall.return_value = rows or []
    result.scalar.return_value   = scalar
    session.execute.return_value = result
    return session


def test_pipeline_por_stage():
    session = _make_session(rows=[("Lead Capturado", 5), ("Qualificação", 3)])
    out = _pipeline_por_stage(session)
    assert out == {"Lead Capturado": 5, "Qualificação": 3}


def test_deals_criados_no_periodo():
    session = _make_session(scalar=7)
    count = _deals_criados_no_periodo(session, datetime.utcnow() - timedelta(days=1), datetime.utcnow())
    assert count == 7


def test_deals_em_qualificacao_vazio():
    session = _make_session(rows=[])
    out = _deals_em_qualificacao(session)
    assert out == []


def test_deals_em_qualificacao_com_dados():
    session = _make_session(rows=[
        ("DEAL-001", "SOJA", "Leonardo", datetime.utcnow()),
        ("DEAL-002", "MILHO", "Nivio", datetime.utcnow()),
    ])
    out = _deals_em_qualificacao(session)
    assert len(out) == 2
    assert out[0]["name"] == "DEAL-001"


def test_followups_vencidos():
    session = _make_session(scalar=4)
    assert _followups_vencidos(session) == 4


def test_deals_stale_sem_followup():
    session = _make_session(rows=[
        (1, "DEAL-X", "SOJA", "Leonardo", "+55119", "GrupoA"),
    ])
    out = _deals_stale_sem_followup(session, horas=2)
    assert len(out) == 1
    assert out[0]["name"] == "DEAL-X"


def test_deals_stale_vazio():
    session = _make_session(rows=[])
    out = _deals_stale_sem_followup(session)
    assert out == []


# ──────────────────────────────────────────────────────────────────
# Tasks (notifier + session mockados)
# ──────────────────────────────────────────────────────────────────

@patch("tasks.email_tasks._get_session")
@patch("services.internal_notify.get_notifier")
def test_task_morning_brief_ok(mock_notifier_factory, mock_get_session):
    session = _make_session(rows=[], scalar=0)
    mock_get_session.return_value = session

    notifier = MagicMock()
    notifier.send_morning_brief.return_value = True
    mock_notifier_factory.return_value = notifier

    # Simula task sem bind (apply direto)
    result = task_morning_brief.run()
    assert result["status"] == "ok"
    notifier.send_morning_brief.assert_called_once()


@patch("tasks.email_tasks._get_session")
@patch("services.internal_notify.get_notifier")
def test_task_eod_closing_ok(mock_notifier_factory, mock_get_session):
    session = _make_session(rows=[], scalar=0)
    mock_get_session.return_value = session

    notifier = MagicMock()
    notifier.send_eod_closing.return_value = True
    mock_notifier_factory.return_value = notifier

    result = task_eod_closing.run()
    assert result["status"] == "ok"


@patch("tasks.email_tasks._get_session")
@patch("services.internal_notify.get_notifier")
def test_task_intraday_alert_sem_pendencias(mock_notifier_factory, mock_get_session):
    session = _make_session(rows=[])
    mock_get_session.return_value = session

    result = task_intraday_alert.run()
    # Nenhum alerta enviado — pipeline limpo
    assert result["stale_count"] == 0
    mock_notifier_factory.assert_not_called()


@patch("tasks.email_tasks._get_session")
@patch("services.internal_notify.get_notifier")
def test_task_intraday_alert_com_pendencias(mock_notifier_factory, mock_get_session):
    session = _make_session(rows=[
        (1, "DEAL-A", "SOJA", "Leonardo", "+55119", "Grupo"),
        (2, "DEAL-B", "MILHO", "Nivio", "+55118", "Grupo2"),
    ])
    mock_get_session.return_value = session

    notifier = MagicMock()
    notifier.send_intraday_alert.return_value = True
    mock_notifier_factory.return_value = notifier

    result = task_intraday_alert.run()
    assert result["stale_count"] == 2
    notifier.send_intraday_alert.assert_called_once()
