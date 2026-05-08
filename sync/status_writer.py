"""
sync/status_writer.py
=====================
Escrita transacional do status na coluna "STATUS_AUTOMACAO" (O) da planilha,
com suporte opcional a uma coluna acessória de "extrato" (P) — usada pelo
orquestrador para deixar visível ao Trading Desk o resumo da cotação processada.

A planilha é a superfície visível do pipeline — a equipa comercial abre a aba
e vê imediatamente quais linhas foram OK, quais ficaram PENDING_IA e quais
foram REJECTED — sem precisar abrir logs.

APIs:
  - `mark(row_index, status)`                                          — 1 célula
  - `mark_batch([(row, status), ...])`                                 — N linhas, 1 HTTP
  - `mark_batch_with_extras([(row, status, extra), ...], extras_column="P")`
                                                                       — escreve O+P juntos
"""
from __future__ import annotations

import logging
from typing import Any, Iterable

from sync.status import SyncStatus

logger = logging.getLogger("SambaStatusWriter")


class SheetStatusWriter:
    """
    Wrapper fino sobre `sheets_service.spreadsheets().values()`.

    Não faz retry próprio (delegado ao transporte do google-api-python-client).
    Se a escrita falhar, o orquestrador loga e segue — a linha será reprocessada
    na próxima execução (lookup em coluna O vazia ou != OK).
    """

    def __init__(
        self,
        sheets_service: Any,
        spreadsheet_id: str,
        sheet_name: str,
        status_column: str = "O",
        extras_column: str = "P",
    ) -> None:
        self._sheets = sheets_service
        self._spreadsheet_id = spreadsheet_id
        self._sheet_name = sheet_name
        self._status_column = status_column
        self._extras_column = extras_column

    # --- API pública ---------------------------------------------------------

    def mark(self, row_index: int, status: SyncStatus) -> None:
        """Escreve o status em uma única célula (`<sheet>!<col><row>`)."""
        rng = self._cell_range(row_index)
        self._sheets.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range=rng,
            valueInputOption="RAW",
            body={"values": [[status.value]]},
        ).execute()
        logger.debug("Status [%s] gravado em %s", status.value, rng)

    def mark_batch(self, updates: Iterable[tuple[int, SyncStatus]]) -> int:
        """
        Grava N linhas em uma única requisição. Retorna quantas foram enviadas.
        Ideal para o fim do loop do orquestrador — reduz tráfego de Sheets API.
        """
        data = [
            {
                "range": self._cell_range(row_index),
                "values": [[status.value]],
            }
            for row_index, status in updates
        ]
        if not data:
            return 0

        self._sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()
        logger.info("🗂️  Status escrito em %d linhas da planilha.", len(data))
        return len(data)

    def mark_batch_with_extras(
        self,
        updates: Iterable[tuple[int, SyncStatus, str]],
    ) -> int:
        """
        Grava STATUS (coluna O) + EXTRATO (coluna P) em uma única requisição.

        O `extra` é texto livre (multilinha, com emojis) — usamos
        `valueInputOption="USER_ENTERED"` para que o Sheets respeite quebras
        de linha e formatação visual no Trading Desk.
        """
        data = [
            {
                "range": self._row_range(row_index),
                "values": [[status.value, extra or ""]],
            }
            for row_index, status, extra in updates
        ]
        if not data:
            return 0

        self._sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=self._spreadsheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": data},
        ).execute()
        logger.info(
            "🗂️  Status+Extrato escritos em %d linhas (%s:%s).",
            len(data),
            self._status_column,
            self._extras_column,
        )
        return len(data)

    # --- Helpers -------------------------------------------------------------

    def _cell_range(self, row_index: int) -> str:
        if row_index < 1:
            raise ValueError(f"row_index deve ser >= 1, recebido {row_index}")
        return f"{self._sheet_name}!{self._status_column}{row_index}"

    def _row_range(self, row_index: int) -> str:
        """Range que cobre da coluna de status até a coluna de extras (ex.: O5:P5)."""
        if row_index < 1:
            raise ValueError(f"row_index deve ser >= 1, recebido {row_index}")
        return (
            f"{self._sheet_name}!{self._status_column}{row_index}"
            f":{self._extras_column}{row_index}"
        )
