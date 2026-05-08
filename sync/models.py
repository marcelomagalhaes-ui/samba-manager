"""
sync/models.py
==============
Schemas Pydantic para o pipeline de conciliação Sheets → Drive.

`DealRow` representa uma linha validada da aba "todos andamento" (colunas A:N).
O objetivo é barrar linhas malformadas ANTES de gastar cota do Gemini:
  - linhas sem produto são classificadas como `skipped` (esperado, não é erro)
  - linhas que violam o schema são classificadas como `rejected` (alerta)
  - linhas válidas vão para `actionable` e seguem o pipeline

Campos brutos (índice da coluna na planilha):
  0=job_id, 1=data_entrada, 3=grupo, 4=solicitante, 6=produto_raw,
  7=comprador, 9=visao_rapida, 11=especificacao.
  Colunas 2, 5, 8, 10, 12, 13 existem mas são reservadas/ignoradas.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# Ordem fixa das colunas na planilha "todos andamento" (A:N)
SHEET_COLUMN_COUNT = 14


class DealRow(BaseModel):
    """Uma linha validada da planilha, pronta para alimentar o orquestrador."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    row_index: int = Field(
        ge=1,
        description="Linha 1-based na planilha (para logs, DLQ e reconciliação).",
    )
    job_id: str = ""
    data_entrada: str = ""
    grupo: str = ""
    solicitante: str = ""
    produto_raw: str = ""
    comprador: str = ""
    visao_rapida: str = ""
    especificacao: str = ""

    @classmethod
    def from_sheet_row(cls, values: list[Any], row_index: int) -> "DealRow":
        """Mapeia a lista bruta (A..N) da Sheets API para campos tipados."""
        padded = [""] * SHEET_COLUMN_COUNT
        for i, v in enumerate(list(values)[:SHEET_COLUMN_COUNT]):
            padded[i] = str(v if v is not None else "").strip()

        return cls(
            row_index=row_index,
            job_id=padded[0],
            data_entrada=padded[1],
            grupo=padded[3],
            solicitante=padded[4],
            produto_raw=padded[6],
            comprador=padded[7],
            visao_rapida=padded[9],
            especificacao=padded[11],
        )

    # -------------------------------------------------------------------
    # Derivados — centralizam regras de negócio que estavam no monolito
    # -------------------------------------------------------------------

    @property
    def is_actionable(self) -> bool:
        """Linhas sem produto são ignoradas (comportamento do monolito)."""
        return bool(self.produto_raw)

    @property
    def entity(self) -> str:
        """Prioridade da entidade comercial: comprador > solicitante > grupo > placeholder."""
        candidate = self.comprador or self.solicitante or self.grupo or "PARCEIRO N/D"
        return candidate.strip().upper()

    @property
    def free_text_for_llm(self) -> str:
        return f"Visão Rápida: {self.visao_rapida}\nEspecificação Detalhada: {self.especificacao}"

    @property
    def has_llm_context(self) -> bool:
        """Se não há nem Visão nem Especificação, não vale pagar cota de IA."""
        return bool(self.visao_rapida or self.especificacao)


class RowRejection(BaseModel):
    """Linha que não entrou no pipeline — razão e valores brutos para auditoria."""
    model_config = ConfigDict(frozen=True)

    row_index: int
    reason: str
    raw_values: list[str] = Field(default_factory=list)


class IngestionResult(BaseModel):
    """Resultado de um batch de linhas do Sheets, já classificadas."""
    model_config = ConfigDict(frozen=True)

    actionable: list[DealRow] = Field(default_factory=list)
    skipped: list[RowRejection] = Field(default_factory=list)
    rejected: list[RowRejection] = Field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.actionable) + len(self.skipped) + len(self.rejected)


def ingest_sheet_rows(
    raw_rows: list[list[Any]],
    start_row: int = 5,
) -> IngestionResult:
    """
    Valida um batch cru vindo da Sheets API.

    - `start_row` é o número da primeira linha na planilha (default 5 = A5, igual ao monolito).
    - Linhas sem produto viram `skipped`.
    - Linhas que explodem no schema viram `rejected` (com a razão).
    """
    actionable: list[DealRow] = []
    skipped: list[RowRejection] = []
    rejected: list[RowRejection] = []

    for offset, row in enumerate(raw_rows):
        row_idx = start_row + offset
        raw_snapshot = [str(v if v is not None else "") for v in (row or [])]

        try:
            deal = DealRow.from_sheet_row(row or [], row_index=row_idx)
        except Exception as e:
            rejected.append(
                RowRejection(row_index=row_idx, reason=str(e), raw_values=raw_snapshot)
            )
            continue

        if not deal.is_actionable:
            skipped.append(
                RowRejection(
                    row_index=row_idx,
                    reason="produto em branco",
                    raw_values=raw_snapshot,
                )
            )
            continue

        actionable.append(deal)

    return IngestionResult(actionable=actionable, skipped=skipped, rejected=rejected)
