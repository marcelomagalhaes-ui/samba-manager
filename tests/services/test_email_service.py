"""
tests/services/test_email_service.py
=====================================
Testes unitários do EmailService (Gmail API mockada).
"""
from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import pytest

from services.email_service import EmailService, _section, _row, _deal_pill


# ──────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_creds():
    creds = MagicMock()
    creds.valid = True
    return creds


@pytest.fixture
def email_svc(mock_creds):
    with patch("services.email_service.drive_manager") as mock_dm, \
         patch("services.email_service.build") as mock_build:
        mock_dm.creds = mock_creds
        mock_gmail = MagicMock()
        mock_build.return_value = mock_gmail
        svc = EmailService()
        svc._mock_gmail = mock_gmail  # expõe para asserções
        yield svc


# ──────────────────────────────────────────────────────────────────
# Helpers de template
# ──────────────────────────────────────────────────────────────────

def test_section_contém_título():
    html = _section("PIPELINE", "<p>conteudo</p>")
    assert "PIPELINE" in html
    assert "conteudo" in html


def test_row_highlight_usa_cor_branca():
    html = _row("Label", "Valor", highlight=True)
    assert "#f5f5f7" in html
    assert "Valor" in html


def test_deal_pill_cor_qualificacao():
    html = _deal_pill("DEAL-001", "SOJA", "Qualificação", "Leonardo")
    assert "#e67e22" in html
    assert "DEAL-001" in html
    assert "Leonardo" in html


def test_build_html_inclui_campos_chave():
    html = EmailService.build_html(
        title="Briefing",
        subtitle="Subtítulo",
        body_html="<p>corpo</p>",
        icon="🌅",
    )
    assert "Briefing" in html
    assert "SAMBA EXPORT" in html
    assert "🌅" in html
    assert "corpo" in html


# ──────────────────────────────────────────────────────────────────
# send_html
# ──────────────────────────────────────────────────────────────────

def test_send_html_chama_gmail_api(email_svc):
    email_svc._mock_gmail.users.return_value.messages.return_value \
        .send.return_value.execute.return_value = {"id": "MSG123"}

    ok = email_svc.send_html(
        to="leonardo@sambaexport.com.br",
        subject="Teste",
        html_body="<p>Olá</p>",
    )
    assert ok is True
    email_svc._mock_gmail.users.return_value.messages.return_value \
        .send.assert_called_once()


def test_send_html_com_cc(email_svc):
    email_svc._mock_gmail.users.return_value.messages.return_value \
        .send.return_value.execute.return_value = {}

    ok = email_svc.send_html(
        to="nivio@sambaexport.com.br",
        subject="CC test",
        html_body="<p>body</p>",
        cc=["agente@sambaexport.com.br"],
    )
    assert ok is True


def test_send_html_retorna_false_quando_servico_inativo():
    with patch("services.email_service.drive_manager") as mock_dm:
        mock_dm.creds = None
        svc = EmailService()

    ok = svc.send_html("x@y.com", "assunto", "<p>body</p>")
    assert ok is False


def test_send_html_retorna_false_em_excecao(email_svc):
    email_svc._mock_gmail.users.return_value.messages.return_value \
        .send.return_value.execute.side_effect = Exception("quota exceeded")

    ok = email_svc.send_html("x@y.com", "Falha", "<p>body</p>")
    assert ok is False


def test_send_html_lista_destinatarios(email_svc):
    email_svc._mock_gmail.users.return_value.messages.return_value \
        .send.return_value.execute.return_value = {}

    ok = email_svc.send_html(
        to=["a@sambaexport.com.br", "b@sambaexport.com.br"],
        subject="Multi",
        html_body="<p>x</p>",
    )
    assert ok is True
