"""
Testes do GoogleSheetsSync — foco na shape da linha (A..N) e no append
com cliente Google mockado.
"""
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))


# ----------------------------------------------------------------------------
# _build_row — puro, sem cliente Google
# ----------------------------------------------------------------------------

def test_build_row_bid_completo():
    from services.google_sheets_sync import GoogleSheetsSync

    row = GoogleSheetsSync._build_row({
        "name": "SOJA-001",
        "created_at": datetime(2026, 4, 18, 20, 30),
        "direcao": "BID",
        "source_group": "Samba x Eric",
        "source_sender": "+5511999990001",
        "stage": "Qualificação",
        "commodity": "SOJA",
        "destination": "Santos",
        "origin": "Rondonopolis",
        "volume": 5000,
        "volume_unit": "MT",
        "incoterm": "FOB",
        "price": 420.5,
        "currency": "USD",
        "original_text": "Compramos 5000 ton SOJA FOB Santos a 420.5 USD",
    })
    assert len(row) == 14  # A..N
    assert row[0] == "SOJA-001"                                  # A JOB
    assert row[1] == "18/04/2026"                                # B DATA
    assert row[2] == "Pedido"                                    # C Bid->Pedido
    assert row[3] == "Samba x Eric"                              # D GRUPO
    assert row[5] == "Qualificação"                              # F STATUS
    assert row[6] == "SOJA"                                      # G PRODUTO
    assert row[9] == "5000 MT | FOB | USD 420.5"                 # J VIZ_RAPIDA
    assert row[10] == ""                                         # K DOCS humano
    assert "SOJA FOB Santos" in row[11]                          # L ESPEC
    assert row[12] == "" and row[13] == ""                       # M, N humanos


def test_build_row_ask_incompleto_gera_target():
    """ASK sem preco nem volume deve gerar 'Target?' e '?' no sumario."""
    from services.google_sheets_sync import GoogleSheetsSync

    row = GoogleSheetsSync._build_row({
        "name": "MILHO-X",
        "direcao": "ASK",
        "commodity": "MILHO",
    })
    assert row[2] == "Oferta"
    assert row[9] == "? | ? | Target?"


def test_build_row_direcao_desconhecida():
    from services.google_sheets_sync import GoogleSheetsSync
    row = GoogleSheetsSync._build_row({"name": "x", "direcao": "UNKNOWN"})
    assert row[2] == "Indefinido"


# ----------------------------------------------------------------------------
# append_deal_to_sheet — com cliente Google inteiramente mockado
# ----------------------------------------------------------------------------

@pytest.fixture
def fake_sheets_service():
    service = MagicMock()
    # service.spreadsheets().values().append(...).execute() -> {"updates": {"updatedRange": "..."}}
    service.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {
        "updates": {"updatedRange": "todos andamento!A42:N42"}
    }
    return service


def test_append_deal_devolve_true_e_usa_range_correto(fake_sheets_service, monkeypatch):
    """
    O append deve usar A5:N (nao tocar linhas 1-4 de cabecalho e nao tocar
    colunas O/P reservadas pro SpreadsheetSyncAgent).
    """
    from services import google_sheets_sync as mod

    sync = mod.GoogleSheetsSync.__new__(mod.GoogleSheetsSync)
    sync.service = fake_sheets_service

    ok = sync.append_deal_to_sheet({
        "name": "DEAL-X", "direcao": "BID", "commodity": "SOJA", "price": 420,
    })
    assert ok is True

    append_call = fake_sheets_service.spreadsheets.return_value.values.return_value.append
    kwargs = append_call.call_args.kwargs
    assert kwargs["spreadsheetId"] == mod.SPREADSHEET_ID
    assert kwargs["range"].endswith("!A5:N"), f"range deve terminar com A5:N, veio: {kwargs['range']}"
    assert kwargs["valueInputOption"] == "USER_ENTERED"
    assert kwargs["insertDataOption"] == "INSERT_ROWS"
    assert len(kwargs["body"]["values"]) == 1
    assert len(kwargs["body"]["values"][0]) == 14  # A..N


def test_append_deal_servico_inativo_retorna_false():
    from services.google_sheets_sync import GoogleSheetsSync
    sync = GoogleSheetsSync.__new__(GoogleSheetsSync)
    sync.service = None
    assert sync.append_deal_to_sheet({"name": "x"}) is False


def test_append_deal_erro_google_retorna_false(fake_sheets_service):
    from services.google_sheets_sync import GoogleSheetsSync
    fake_sheets_service.spreadsheets.return_value.values.return_value.append.side_effect = RuntimeError("boom")
    sync = GoogleSheetsSync.__new__(GoogleSheetsSync)
    sync.service = fake_sheets_service
    assert sync.append_deal_to_sheet({"name": "x", "direcao": "BID"}) is False


def test_spreadsheet_id_oficial_default():
    """Default deve ser a planilha oficial (1ToN...), nao a antiga (1ZHY...)."""
    from services.google_sheets_sync import SPREADSHEET_ID
    assert SPREADSHEET_ID.startswith("1ToN"), f"ID default mudou para {SPREADSHEET_ID}"


def test_spreadsheet_id_overridable_via_env(monkeypatch):
    """SAMBA_SHEETS_ID na env sobrescreve o default (para sandbox/testes)."""
    monkeypatch.setenv("SAMBA_SHEETS_ID", "SANDBOX_ID")
    import importlib
    import services.google_sheets_sync as mod
    importlib.reload(mod)
    try:
        assert mod.SPREADSHEET_ID == "SANDBOX_ID"
    finally:
        # Limpa env e recarrega para nao contaminar outros testes.
        monkeypatch.delenv("SAMBA_SHEETS_ID", raising=False)
        importlib.reload(mod)
