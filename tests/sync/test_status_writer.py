"""
Testes do SheetStatusWriter.
Usamos um fake do `sheets_service` que grava o que foi chamado.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from sync import SheetStatusWriter, SyncStatus


# ---------------------------------------------------------------------------
# Fake do cliente Google Sheets — registra chamadas e payloads
# ---------------------------------------------------------------------------

class FakeExecute:
    def __init__(self, parent, method, kwargs):
        self.parent = parent
        self.method = method
        self.kwargs = kwargs

    def execute(self):
        self.parent.calls.append((self.method, self.kwargs))
        return {"ok": True}


class FakeValues:
    def __init__(self, parent):
        self.parent = parent

    def update(self, **kwargs):
        return FakeExecute(self.parent, "update", kwargs)

    def batchUpdate(self, **kwargs):
        return FakeExecute(self.parent, "batchUpdate", kwargs)


class FakeSpreadsheets:
    def __init__(self, parent):
        self._v = FakeValues(parent)

    def values(self):
        return self._v


class FakeSheetsService:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def spreadsheets(self):
        return FakeSpreadsheets(self)


@pytest.fixture
def svc():
    return FakeSheetsService()


@pytest.fixture
def writer(svc):
    return SheetStatusWriter(
        sheets_service=svc,
        spreadsheet_id="SHEET_ID_FAKE",
        sheet_name="todos andamento",
        status_column="O",
    )


# ---------------------------------------------------------------------------
# mark() — escrita unitária
# ---------------------------------------------------------------------------

def test_mark_writes_single_cell(svc, writer):
    writer.mark(row_index=7, status=SyncStatus.OK)

    assert len(svc.calls) == 1
    method, kwargs = svc.calls[0]
    assert method == "update"
    assert kwargs["spreadsheetId"] == "SHEET_ID_FAKE"
    assert kwargs["range"] == "todos andamento!O7"
    assert kwargs["valueInputOption"] == "RAW"
    assert kwargs["body"] == {"values": [["OK"]]}


def test_mark_pending_ia_writes_correct_string(svc, writer):
    writer.mark(row_index=42, status=SyncStatus.PENDING_IA)
    _, kwargs = svc.calls[0]
    assert kwargs["body"]["values"][0][0] == "PENDING_IA"


def test_mark_rejected_writes_rejected_string(svc, writer):
    writer.mark(row_index=99, status=SyncStatus.REJECTED)
    _, kwargs = svc.calls[0]
    assert kwargs["body"]["values"][0][0] == "REJECTED"


def test_mark_rejects_invalid_row_index(svc, writer):
    with pytest.raises(ValueError):
        writer.mark(row_index=0, status=SyncStatus.OK)


# ---------------------------------------------------------------------------
# mark_batch() — um HTTP call para N linhas
# ---------------------------------------------------------------------------

def test_mark_batch_sends_single_request(svc, writer):
    n = writer.mark_batch([
        (5, SyncStatus.OK),
        (6, SyncStatus.PENDING_IA),
        (7, SyncStatus.REJECTED),
    ])
    assert n == 3
    assert len(svc.calls) == 1

    method, kwargs = svc.calls[0]
    assert method == "batchUpdate"
    assert kwargs["spreadsheetId"] == "SHEET_ID_FAKE"

    body = kwargs["body"]
    assert body["valueInputOption"] == "RAW"
    assert len(body["data"]) == 3
    assert body["data"][0]["range"] == "todos andamento!O5"
    assert body["data"][0]["values"] == [["OK"]]
    assert body["data"][1]["range"] == "todos andamento!O6"
    assert body["data"][1]["values"] == [["PENDING_IA"]]
    assert body["data"][2]["values"] == [["REJECTED"]]


def test_mark_batch_empty_is_noop(svc, writer):
    n = writer.mark_batch([])
    assert n == 0
    assert svc.calls == []


# ---------------------------------------------------------------------------
# Coluna customizada
# ---------------------------------------------------------------------------

def test_custom_status_column(svc):
    w = SheetStatusWriter(
        sheets_service=svc,
        spreadsheet_id="SHEET",
        sheet_name="todos andamento",
        status_column="P",
    )
    w.mark(row_index=10, status=SyncStatus.OK)
    _, kwargs = svc.calls[0]
    assert kwargs["range"] == "todos andamento!P10"


def test_sheet_name_with_spaces_is_used_verbatim(svc, writer):
    writer.mark(row_index=5, status=SyncStatus.OK)
    _, kwargs = svc.calls[0]
    # Google Sheets API aceita range literal; aspas não são necessárias em update()
    assert kwargs["range"] == "todos andamento!O5"


# ---------------------------------------------------------------------------
# mark_batch_with_extras() — escreve STATUS (O) + EXTRATO (P) numa única chamada
# ---------------------------------------------------------------------------

def test_mark_batch_with_extras_writes_status_and_extract(svc, writer):
    n = writer.mark_batch_with_extras([
        (5, SyncStatus.OK, "⚖️ VOL: 20x40\n💲 PREÇO: USD 1200"),
        (6, SyncStatus.PENDING_IA, ""),
        (7, SyncStatus.REJECTED, "Sem dados comerciais"),
    ])
    assert n == 3
    assert len(svc.calls) == 1

    method, kwargs = svc.calls[0]
    assert method == "batchUpdate"
    assert kwargs["spreadsheetId"] == "SHEET_ID_FAKE"

    body = kwargs["body"]
    # Extrato pode conter quebras de linha/emojis — precisa de USER_ENTERED.
    assert body["valueInputOption"] == "USER_ENTERED"
    assert len(body["data"]) == 3

    assert body["data"][0]["range"] == "todos andamento!O5:P5"
    assert body["data"][0]["values"] == [["OK", "⚖️ VOL: 20x40\n💲 PREÇO: USD 1200"]]
    assert body["data"][1]["range"] == "todos andamento!O6:P6"
    assert body["data"][1]["values"] == [["PENDING_IA", ""]]
    assert body["data"][2]["values"] == [["REJECTED", "Sem dados comerciais"]]


def test_mark_batch_with_extras_empty_is_noop(svc, writer):
    n = writer.mark_batch_with_extras([])
    assert n == 0
    assert svc.calls == []


def test_mark_batch_with_extras_uses_custom_columns(svc):
    w = SheetStatusWriter(
        sheets_service=svc,
        spreadsheet_id="SHEET",
        sheet_name="todos andamento",
        status_column="O",
        extras_column="Q",
    )
    w.mark_batch_with_extras([(10, SyncStatus.OK, "extrato")])
    _, kwargs = svc.calls[0]
    assert kwargs["body"]["data"][0]["range"] == "todos andamento!O10:Q10"


def test_mark_batch_with_extras_treats_none_as_empty_string(svc, writer):
    writer.mark_batch_with_extras([(5, SyncStatus.OK, None)])  # type: ignore[arg-type]
    _, kwargs = svc.calls[0]
    assert kwargs["body"]["data"][0]["values"] == [["OK", ""]]
