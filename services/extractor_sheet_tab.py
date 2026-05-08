"""
services/extractor_sheet_tab.py
================================
Sincroniza a aba "extractor" da planilha oficial com os deals que entraram
no sistema mas NÃO têm dados comerciais completos:

  - commodity = Indefinida / vazia / None
  - direcao   = UNKNOWN
  - price     = 0 / None
  - volume    = None

Esses deals ficam em "Qualificação" e precisam de intervenção humana.
A aba serve de painel de controle para os sócios saberem exatamente
o que está pendente de complementação.

Colunas da aba (A-J):
  A  ID           (deal.id)
  B  Deal         (deal.name)
  C  Data         (created_at)
  D  Remetente    (source_sender)
  E  Grupo WPP    (source_group)
  F  Commodity    (extraído — pode estar errado/vazio)
  G  Direção      (BID/ASK/UNKNOWN)
  H  Preço        (ou "–")
  I  Volume       (ou "–")
  J  Msg Original (primeiros 300 chars do content da mensagem)

A aba é RECRIADA a cada execução (limpa + reescreve) para refletir
o estado atual do banco. Nunca toca outras abas.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from googleapiclient.discovery import build
from services.google_drive import drive_manager

logger = logging.getLogger("ExtractorSheetTab")

SPREADSHEET_ID: str = os.getenv(
    "SAMBA_SHEETS_ID",
    "1ToNZxYYi0dPQkQ0bRE8W3DWXJxkzEVS4vLQyrj2VP9U",
)
TAB_NAME = "extractor"

# Paleta de destaque para células com problemas
COLOR_HEADER   = {"red": 0.055, "green": 0.055, "blue": 0.043}   # #0e0e0b
COLOR_GOLD     = {"red": 0.831, "green": 0.686, "blue": 0.216}   # #d4af37
COLOR_ORANGE   = {"red": 0.902, "green": 0.494, "blue": 0.133}   # #e67e22 — UNKNOWN/vazio
COLOR_ROW_EVEN = {"red": 0.082, "green": 0.082, "blue": 0.094}
COLOR_ROW_ODD  = {"red": 0.094, "green": 0.094, "blue": 0.106}


class ExtractorSheetTab:
    """Gerencia a aba 'extractor' da planilha oficial."""

    def __init__(self) -> None:
        self.sheets_svc: Any = None
        self._sheet_id: int | None = None   # gid interno da aba
        self._init()

    def _init(self) -> None:
        if not drive_manager.creds or not drive_manager.creds.valid:
            logger.error("Credenciais inválidas — ExtractorSheetTab inativo.")
            return
        try:
            self.sheets_svc = build("sheets", "v4", credentials=drive_manager.creds)
            self._sheet_id = self._get_or_create_tab()
        except Exception as exc:
            logger.error("Falha ao inicializar ExtractorSheetTab: %s", exc)

    # ── Utilitários de aba ───────────────────────────────────────────────

    def _get_or_create_tab(self) -> int:
        """Retorna o sheetId (gid) da aba 'extractor', criando se não existir."""
        meta = self.sheets_svc.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID
        ).execute()

        for sheet in meta["sheets"]:
            if sheet["properties"]["title"] == TAB_NAME:
                return sheet["properties"]["sheetId"]

        # Cria a aba
        resp = self.sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB_NAME}}}]},
        ).execute()
        new_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
        logger.info("Aba '%s' criada (sheetId=%s).", TAB_NAME, new_id)
        return new_id

    def _clear_tab(self) -> None:
        """Limpa todo o conteúdo da aba antes de reescrever."""
        self.sheets_svc.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_NAME}!A:Z",
        ).execute()

    # ── Construção das linhas ────────────────────────────────────────────

    @staticmethod
    def _load_incomplete_deals() -> list[dict[str, Any]]:
        """
        Busca no SQLite os deals sem dados comerciais completos
        (em Qualificação ou com UNKNOWN/vazio em campos críticos).
        """
        from models.database import get_session
        from sqlalchemy import text
        session = get_session()
        try:
            rows = session.execute(text("""
                SELECT
                    id, name, created_at, source_sender, source_group,
                    commodity, direcao, price, volume, notes
                FROM deals
                WHERE status = 'ativo'
                  AND (
                    stage = 'Qualificação'
                    OR UPPER(COALESCE(direcao,'')) = 'UNKNOWN'
                    OR LOWER(COALESCE(commodity,'')) IN ('indefinida','indefinido','')
                    OR price IS NULL OR price = 0
                    OR volume IS NULL OR volume = 0
                  )
                ORDER BY created_at DESC
                LIMIT 500
            """)).fetchall()
            result = []
            for r in rows:
                # Extrai texto original das notes
                notes = r[9] or ""
                orig = ""
                if "[WHATSAPP] Texto Original:" in notes:
                    after = notes.split("[WHATSAPP] Texto Original:")[-1]
                    orig = after.split("[")[0].strip()[:300]
                result.append({
                    "id": r[0], "name": r[1] or "–",
                    "created_at": r[2], "sender": r[3] or "–",
                    "group": r[4] or "–", "commodity": r[5] or "–",
                    "direcao": r[6] or "UNKNOWN",
                    "price": r[7], "volume": r[8], "orig": orig,
                })
            return result
        finally:
            session.close()

    @staticmethod
    def _fmt_date(dt) -> str:
        if isinstance(dt, datetime):
            return dt.strftime("%d/%m/%Y %H:%M")
        return str(dt) if dt else "–"

    @staticmethod
    def _fmt_num(v) -> str:
        if v is None or v == 0:
            return "–"
        return f"{v:,.0f}".replace(",", ".")

    def _build_rows(self, deals: list[dict]) -> list[list[str]]:
        rows = []
        for d in deals:
            rows.append([
                str(d["id"]),
                d["name"],
                self._fmt_date(d["created_at"]),
                d["sender"],
                d["group"],
                d["commodity"],
                d["direcao"],
                self._fmt_num(d["price"]),
                self._fmt_num(d["volume"]),
                d["orig"],
            ])
        return rows

    # ── Formatação visual ────────────────────────────────────────────────

    def _apply_formatting(self, total_rows: int) -> None:
        """Aplica cores, negrito e larguras de coluna na aba."""
        if self._sheet_id is None:
            return

        requests = [
            # Larguras das colunas
            {"updateDimensionProperties": {
                "range": {"sheetId": self._sheet_id, "dimension": "COLUMNS",
                          "startIndex": 0, "endIndex": 10},
                "properties": {"pixelSize": 130}, "fields": "pixelSize",
            }},
            # Coluna J (msg original) mais larga
            {"updateDimensionProperties": {
                "range": {"sheetId": self._sheet_id, "dimension": "COLUMNS",
                          "startIndex": 9, "endIndex": 10},
                "properties": {"pixelSize": 380}, "fields": "pixelSize",
            }},
            # Header: fundo escuro, texto dourado, negrito
            {"repeatCell": {
                "range": {"sheetId": self._sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": COLOR_HEADER,
                    "textFormat": {"foregroundColor": COLOR_GOLD, "bold": True, "fontSize": 10},
                    "horizontalAlignment": "CENTER",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }},
            # Congela linha 1
            {"updateSheetProperties": {
                "properties": {"sheetId": self._sheet_id,
                               "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }},
        ]

        # Linhas de dados: zebra alternada
        for i in range(1, total_rows + 1):
            color = COLOR_ROW_ODD if i % 2 else COLOR_ROW_EVEN
            requests.append({"repeatCell": {
                "range": {"sheetId": self._sheet_id,
                          "startRowIndex": i, "endRowIndex": i + 1,
                          "startColumnIndex": 0, "endColumnIndex": 10},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": color,
                    "textFormat": {"foregroundColor": {"red": 0.96, "green": 0.96, "blue": 0.97}, "fontSize": 9},
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }})

        self.sheets_svc.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": requests},
        ).execute()

    # ── Método público ───────────────────────────────────────────────────

    def sync(self) -> dict[str, Any]:
        """
        Recria a aba 'extractor' com os deals incompletos atuais.
        Retorna estatísticas da operação.
        """
        if not self.sheets_svc:
            return {"status": "error", "reason": "service_unavailable"}

        deals = self._load_incomplete_deals()
        logger.info("ExtractorSheetTab: %s deals incompletos encontrados.", len(deals))

        self._clear_tab()

        header = [
            "ID", "Deal", "Data", "Remetente", "Grupo WPP",
            "Commodity", "Direção", "Preço", "Volume", "Msg Original (WhatsApp)",
        ]
        data_rows = self._build_rows(deals)
        all_rows = [header] + data_rows

        self.sheets_svc.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{TAB_NAME}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": all_rows},
        ).execute()

        self._apply_formatting(len(data_rows))

        result = {
            "status": "ok",
            "deals_incompletos": len(deals),
            "tab": TAB_NAME,
            "atualizado_em": datetime.utcnow().isoformat(),
        }
        logger.info("ExtractorSheetTab: aba sincronizada. %s", result)
        return result
