"""
Testes dos schemas Pydantic.
Foco: parse correto de linhas cruas do Sheets e classificação em
actionable / skipped / rejected antes de chegar ao Gemini.
"""
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from sync import DealRow, ingest_sheet_rows, IngestionResult, RowRejection


# Row "modelo" com 14 colunas completas (A..N).
FULL_ROW = [
    "JOB-001",           # A job_id
    "17/04/2026",        # B data_entrada
    "CAT-INT",           # C (ignorado)
    "Grupo Global",      # D grupo
    "Leonardo Santos",   # E solicitante
    "CLASSIF-A",         # F (ignorado)
    "Pork Belly",        # G produto_raw
    "Alpha Trading Co",  # H comprador
    "STATUS-1",          # I (ignorado)
    "20x40 FCL, FOB",    # J visao_rapida
    "NOTA-X",            # K (ignorado)
    "IQF, 12-14mm, halal certified",  # L especificacao
    "xxx",               # M (ignorado)
    "yyy",               # N (ignorado)
]


# ---------------------------------------------------------------------------
# from_sheet_row: parsing bem-formado
# ---------------------------------------------------------------------------

def test_parses_full_row():
    deal = DealRow.from_sheet_row(FULL_ROW, row_index=5)
    assert deal.row_index == 5
    assert deal.job_id == "JOB-001"
    assert deal.data_entrada == "17/04/2026"
    assert deal.grupo == "Grupo Global"
    assert deal.solicitante == "Leonardo Santos"
    assert deal.produto_raw == "Pork Belly"
    assert deal.comprador == "Alpha Trading Co"
    assert deal.visao_rapida == "20x40 FCL, FOB"
    assert deal.especificacao == "IQF, 12-14mm, halal certified"


def test_row_shorter_than_14_is_padded():
    short = ["JOB-002", "01/01/2026", "", "Grupo", "", "", "Milho"]
    deal = DealRow.from_sheet_row(short, row_index=10)
    assert deal.produto_raw == "Milho"
    assert deal.comprador == ""
    assert deal.visao_rapida == ""
    assert deal.especificacao == ""


def test_row_with_none_values_is_safe():
    row = [None] * 14
    row[6] = "Soja"
    deal = DealRow.from_sheet_row(row, row_index=1)
    assert deal.produto_raw == "Soja"
    assert deal.grupo == ""


def test_whitespace_is_trimmed():
    row = list(FULL_ROW)
    row[6] = "  Pork Belly  "
    row[7] = "  Alpha Trading Co  "
    deal = DealRow.from_sheet_row(row, row_index=2)
    assert deal.produto_raw == "Pork Belly"
    assert deal.comprador == "Alpha Trading Co"


def test_ignores_columns_beyond_14():
    long_row = FULL_ROW + ["EXTRA-1", "EXTRA-2"]
    deal = DealRow.from_sheet_row(long_row, row_index=5)
    assert deal.produto_raw == "Pork Belly"


# ---------------------------------------------------------------------------
# Propriedades derivadas (regras de negócio)
# ---------------------------------------------------------------------------

def test_is_actionable_true_when_produto_present():
    deal = DealRow.from_sheet_row(FULL_ROW, row_index=5)
    assert deal.is_actionable is True


def test_is_actionable_false_when_produto_empty():
    row = list(FULL_ROW)
    row[6] = ""
    deal = DealRow.from_sheet_row(row, row_index=5)
    assert deal.is_actionable is False


@pytest.mark.parametrize(
    "comprador, solicitante, grupo, expected",
    [
        ("Alpha Trading", "Leonardo", "Grupo X", "ALPHA TRADING"),
        ("", "Leonardo", "Grupo X", "LEONARDO"),
        ("", "", "Grupo X", "GRUPO X"),
        ("", "", "", "PARCEIRO N/D"),
    ],
)
def test_entity_priority(comprador, solicitante, grupo, expected):
    row = list(FULL_ROW)
    row[3] = grupo
    row[4] = solicitante
    row[7] = comprador
    deal = DealRow.from_sheet_row(row, row_index=5)
    assert deal.entity == expected


def test_free_text_for_llm_concatenates():
    deal = DealRow.from_sheet_row(FULL_ROW, row_index=5)
    assert "Visão Rápida: 20x40 FCL, FOB" in deal.free_text_for_llm
    assert "IQF, 12-14mm" in deal.free_text_for_llm


def test_has_llm_context_detects_empty():
    row = list(FULL_ROW)
    row[9] = ""
    row[11] = ""
    deal = DealRow.from_sheet_row(row, row_index=5)
    assert deal.has_llm_context is False


def test_has_llm_context_true_with_only_visao():
    row = list(FULL_ROW)
    row[11] = ""
    deal = DealRow.from_sheet_row(row, row_index=5)
    assert deal.has_llm_context is True


# ---------------------------------------------------------------------------
# Imutabilidade e rigor do schema
# ---------------------------------------------------------------------------

def test_deal_row_is_frozen():
    deal = DealRow.from_sheet_row(FULL_ROW, row_index=5)
    with pytest.raises(ValidationError):
        deal.produto_raw = "Outro Produto"  # type: ignore[misc]


def test_rejects_invalid_row_index():
    with pytest.raises(ValidationError):
        DealRow(row_index=0, produto_raw="Soja")


def test_rejects_extra_fields():
    with pytest.raises(ValidationError):
        DealRow(row_index=5, produto_raw="Soja", campo_fantasma="x")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# ingest_sheet_rows: classificação do batch
# ---------------------------------------------------------------------------

def test_ingest_classifies_actionable_and_skipped():
    rows = [
        FULL_ROW,                             # actionable
        ["", "", "", "", "", "", "", "", "", "", "", "", "", ""],  # skip (sem produto)
        ["JOB-003", "", "", "", "", "", "Cacau", "", "", "", "", "", "", ""],  # actionable
    ]
    result = ingest_sheet_rows(rows, start_row=5)

    assert isinstance(result, IngestionResult)
    assert len(result.actionable) == 2
    assert len(result.skipped) == 1
    assert len(result.rejected) == 0
    assert result.total == 3

    assert result.actionable[0].row_index == 5
    assert result.actionable[1].row_index == 7
    assert result.skipped[0].row_index == 6
    assert result.skipped[0].reason == "produto em branco"


def test_ingest_with_empty_batch():
    result = ingest_sheet_rows([], start_row=5)
    assert result.total == 0


def test_ingest_handles_ragged_rows():
    # Linhas com comprimento variável (normal no Sheets — células vazias à direita somem)
    rows = [
        ["JOB-X", "", "", "", "", "", "Milho"],                   # 7 cols só
        ["JOB-Y", "", "", "", "", "", ""],                        # produto vazio
    ]
    result = ingest_sheet_rows(rows, start_row=10)
    assert len(result.actionable) == 1
    assert len(result.skipped) == 1
    assert result.actionable[0].produto_raw == "Milho"
    assert result.actionable[0].row_index == 10
    assert result.skipped[0].row_index == 11


def test_ingest_snapshot_preserves_raw_values_for_audit():
    rows = [["JOB-Z", "", "", "", "", "", ""]]  # skip
    result = ingest_sheet_rows(rows, start_row=5)
    assert result.skipped[0].raw_values[0] == "JOB-Z"


def test_ingest_handles_none_row():
    # Defensivo: Sheets às vezes devolve None no meio do range.
    rows = [None, FULL_ROW]
    result = ingest_sheet_rows(rows, start_row=5)  # type: ignore[arg-type]
    assert len(result.skipped) == 1
    assert len(result.actionable) == 1
    assert result.actionable[0].row_index == 6
