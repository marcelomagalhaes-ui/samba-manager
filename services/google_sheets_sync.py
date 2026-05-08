"""
services/google_sheets_sync.py
==============================
Motor de sincronizacao com a planilha oficial da Samba Export.

Estrutura de colunas da aba "todos andamento" (A-N):
  A = JOB              (nome canonico do deal)
  B = DATA             (entrada no pipeline)
  C = OFERTA/PEDIDO    (Pedido=BID / Oferta=ASK)
  D = GRUPO            (origem WhatsApp)
  E = SOLICITANTE
  F = STATUS           (stage do pipeline)
  G = PRODUTO          (commodity)
  H = COMPRADOR        (ou porto de destino)
  I = FORNECEDOR       (ou origem)
  J = VIZ_RAPIDA       (sumario Vol | Incoterm | Preco)
  K = DOCS             (reservado humano)
  L = ESPECIFICACAO    (texto original do WhatsApp + link do Drive)
  M = SITUACAO         (reservado humano)
  N = ACAO             (reservado humano)

Observacoes de arquitetura:
  - As colunas O (STATUS_AUTOMACAO) e P (EXTRATO) sao DELIBERADAMENTE
    omitidas deste append. Quem escreve nelas e o `SpreadsheetSyncAgent`,
    que le linhas novas (com O vazio), gera o PDF/pasta no Drive e entao
    marca O=OK + P=<extrato>.
  - O range usado no append e `todos andamento!A5:N`. Linhas 1-4 sao
    titulos/branding e nao sao tocadas. Com `insertDataOption=INSERT_ROWS`,
    a API do Google Sheets acha a ultima linha ocupada dentro do range e
    insere a proxima linha em sequencia.
  - `SPREADSHEET_ID` pode ser sobrescrito via env `SAMBA_SHEETS_ID` para
    permitir testes em planilhas sandbox sem mudar codigo.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from googleapiclient.discovery import build

from services.google_drive import drive_manager

logger = logging.getLogger("GoogleSheetsSync")

# Planilha oficial da operacao (aba "todos andamento" | gid=48539846).
# Sobrescrevivel via env para testes/sandbox.
SPREADSHEET_ID: str = os.getenv(
    "SAMBA_SHEETS_ID",
    "1ToNZxYYi0dPQkQ0bRE8W3DWXJxkzEVS4vLQyrj2VP9U",
)
SHEET_TAB_NAME: str = "todos andamento"
APPEND_RANGE: str = f"{SHEET_TAB_NAME}!A5:N"


class GoogleSheetsSync:
    """
    Cliente magro para o append idempotente de deals na aba oficial.

    Falhas sao logadas e convertidas em retorno `False` — nunca lancam,
    de forma que o callsite (ExtractorAgent) possa commitar o Deal no SQLite
    mesmo que o Sheets esteja indisponivel. O SpreadsheetSyncAgent (Beat)
    reconcilia depois quando a planilha voltar.

    Idempotencia: antes de cada append, le a coluna A (JOB) da aba e verifica
    se o nome do deal ja existe. Se sim, pula. O cache interno _existing_jobs
    e invalidado apos cada append bem-sucedido.
    """

    def __init__(self) -> None:
        self.service: Any = None
        self._existing_jobs: set[str] | None = None  # cache coluna A
        self._initialize_service()

    def _initialize_service(self) -> None:
        """Constroi o cliente Sheets v4 reusando o handshake do DriveManager."""
        if not drive_manager.creds or not drive_manager.creds.valid:
            logger.error("Handshake de credenciais falhou. Verifique o DriveManager.")
            return

        try:
            self.service = build("sheets", "v4", credentials=drive_manager.creds)
            logger.info("Conexao estabelecida com a Google Sheets API v4.")
        except Exception as e:
            logger.error("Falha critica na construcao do servico Sheets: %s", e)

    # ------------------------------------------------------------------
    # Formatacao: deal dict -> linha A..N
    # ------------------------------------------------------------------

    @staticmethod
    def _build_row(deal_data: dict[str, Any]) -> list[str]:
        """
        Converte o dict de um Deal na sequencia A..N esperada pela aba.

        Isolado de `append_deal_to_sheet` para permitir teste unitario sem
        precisar do cliente Google (so shape da linha).
        """
        dt_obj = deal_data.get("created_at")
        data_str = dt_obj.strftime("%d/%m/%Y") if isinstance(dt_obj, datetime) else ""

        direcao = deal_data.get("direcao", "UNKNOWN")
        oferta_pedido = (
            "Pedido" if direcao == "BID"
            else "Oferta" if direcao == "ASK"
            else "Indefinido"
        )

        vol = deal_data.get("volume")
        vol_unit = deal_data.get("volume_unit", "MT")
        vol_str = f"{vol} {vol_unit}" if vol else "?"
        inc_str = deal_data.get("incoterm") or "?"
        price = deal_data.get("price")
        currency = deal_data.get("currency", "USD")
        price_str = f"{currency} {price}" if price else "Target?"
        viz_rapida = f"{vol_str} | {inc_str} | {price_str}"

        return [
            str(deal_data.get("name", "")),                   # A: JOB
            data_str,                                         # B: DATA
            oferta_pedido,                                    # C: OFERTA/PEDIDO
            str(deal_data.get("source_group", "") or ""),     # D: GRUPO
            str(deal_data.get("source_sender", "") or ""),    # E: SOLICITANTE
            str(deal_data.get("stage", "Lead Capturado")),    # F: STATUS
            str(deal_data.get("commodity", "") or ""),        # G: PRODUTO
            str(deal_data.get("destination", "") or ""),      # H: COMPRADOR
            str(deal_data.get("origin", "") or ""),           # I: FORNECEDOR
            viz_rapida,                                       # J: VIZ_RAPIDA
            "",                                               # K: DOCS (humano)
            str(deal_data.get("original_text", "") or ""),    # L: ESPEC / WHATS
            "",                                               # M: SITUACAO (humano)
            "",                                               # N: ACAO (humano)
        ]

    # ------------------------------------------------------------------
    # Append publico
    # ------------------------------------------------------------------

    def _load_existing_jobs(self) -> set[str]:
        """Le a coluna A (JOB) da aba e retorna um set de nomes ja presentes."""
        try:
            resp = self.service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TAB_NAME}!A5:A",
            ).execute()
            rows = resp.get("values", [])
            return {str(r[0]).strip() for r in rows if r and r[0]}
        except Exception as e:
            logger.warning("Nao foi possivel ler coluna A para check de idempotencia: %s", e)
            return set()

    @staticmethod
    def _deal_already_synced_in_db(deal_name: str) -> bool:
        """
        Checa no SQLite se o deal ja tem synced_to_sheets=1.
        Primeira barreira — atomica, sem race condition entre workers Celery.
        """
        if not deal_name:
            return False
        try:
            from models.database import get_session
            from sqlalchemy import text
            session = get_session()
            try:
                row = session.execute(
                    text("SELECT synced_to_sheets FROM deals WHERE name=:n LIMIT 1"),
                    {"n": deal_name},
                ).fetchone()
                return bool(row and row[0])
            finally:
                session.close()
        except Exception as exc:
            logger.warning("Nao foi possivel checar synced_to_sheets no DB: %s", exc)
            return False

    def append_deal_to_sheet(self, deal_data: dict[str, Any]) -> bool:
        """
        Injeta uma nova linha na primeira vaga apos a ultima linha com dados
        do range `todos andamento!A5:N`. Retorna True em sucesso.

        Idempotencia dupla (DB-first):
          1. Checa synced_to_sheets=1 no SQLite (atomico — prova contra race).
          2. Checa coluna A do Sheet (captura casos legados sem flag no banco).
        Deals sem nome (coluna A vazia) sempre sao inseridos.

        Invariante: NUNCA toca colunas O/P - o SpreadsheetSyncAgent as usa
        como sinal de "precisa processar" (O vazio) vs "ja processado" (O=OK).
        """
        if not self.service:
            logger.warning("Sync ignorado: servico Sheets inativo.")
            return False

        deal_name = str(deal_data.get("name", "")).strip()

        # Barreira 1: SQLite (rapido, atomico — sem chamada de rede)
        if self._deal_already_synced_in_db(deal_name):
            logger.info("Deal '%s' ja tem synced_to_sheets=1 no banco — append ignorado.", deal_name)
            return True

        # Barreira 2: coluna A do Sheet (captura deals legados sem flag no banco)
        _jobs = getattr(self, "_existing_jobs", None)
        if deal_name and _jobs is None:
            _jobs = self._load_existing_jobs()
            self._existing_jobs = _jobs

        if deal_name and _jobs is not None and deal_name in _jobs:
            logger.info("Deal '%s' ja existe na planilha — append ignorado.", deal_name)
            return True  # Ja sincronizado, reporta sucesso sem duplicar.

        try:
            nova_linha = self._build_row(deal_data)
            body = {"values": [nova_linha]}

            result = self.service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=APPEND_RANGE,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            ).execute()

            updated = result.get("updates", {}).get("updatedRange", "?")
            logger.info("Pipeline sincronizado. Linha adicionada em %s", updated)

            # Invalida cache para proxima chamada refletir o novo estado.
            if deal_name:
                if self._existing_jobs is not None:
                    self._existing_jobs.add(deal_name)
            return True

        except Exception as e:
            logger.error("Erro de I/O no Google Sheets: %s", e)
            return False
