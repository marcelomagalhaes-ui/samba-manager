"""
agents/spreadsheet_sync_agent.py
================================
Motor de Conciliação Institucional (Top-Down: Sheet -> Drive).

Responsabilidades:
  1. Lê a planilha de controle ("todos andamento") até a coluna P.
  2. Pula linhas já marcadas OK na coluna O (proteção de cota da IA).
  3. Resolve o produto contra o atlas MDM (taxonomy.ProductResolver).
  4. Cria/recupera Deal Room na hierarquia correta no Google Drive.
  5. SEARCH & DESTROY: remove fichas antigas (Ficha/Cadastro) antes de regenerar.
  6. Aciona IA via LLMGateway (com Circuit Breaker — uma falha "desarma o motor"
     pelo restante do run, replicando a regra do disjuntor original).
  7. Gera Ficha de Cadastro em PDF e arquiva no Deal Room.
  8. Escreve STATUS (coluna O) + EXTRATO (coluna P) em batch único ao final.

Regra de ouro:
  Se o LLM falhar, NENHUM PDF é gerado. A linha é marcada PENDING_IA e
  reprocessada na próxima execução.
"""
import sys
import os
import time
import logging
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from googleapiclient.discovery import build

from services.drive_service import DriveService
from services.pdf_service import PDFService
from services.gemini_api import extract_quote_data
from services.google_drive import drive_manager

from sync import (
    CircuitBreaker,
    LLMGateway,
    LLMUnavailable,
    SheetStatusWriter,
    SyncStatus,
    ingest_sheet_rows,
)
from taxonomy import ProductResolver, ResolvedProduct, normalize_text

logging.basicConfig(level=logging.INFO, format='%(levelname)s: [%(name)s] %(message)s')
logger = logging.getLogger("SheetSync")

# Constantes da Arquitetura
SPREADSHEET_ID = '1ToNZxYYi0dPQkQ0bRE8W3DWXJxkzEVS4vLQyrj2VP9U'
SHEET_NAME = 'todos andamento'
RANGE_NAME = f'{SHEET_NAME}!A5:P150'  # até a coluna P (extrato)
SAMBA_ROOT_FOLDER_ID = "1k0uKPg7Xyq8MyI8KI1bRKzR-Bow_41B5"
SAMBA_NEGOCIOS_FOLDER_ID = "1PUEumy3DuF41SPlehFDqVa4j1sr_FliB"
STATUS_COLUMN = 'O'
EXTRATO_COLUMN = 'P'
START_ROW = 5
STATUS_COLUMN_INDEX = 14  # coluna O dentro de A:P (0-based)


def formatar_extrato_enterprise(dados_ia: dict) -> str:
    """Resumo visual da cotação (coluna P) — emojis e quebras de linha."""
    if not isinstance(dados_ia, dict) or not dados_ia:
        return ""
    linhas = []
    vol = dados_ia.get('volume', 'TBI')
    price = dados_ia.get('price', 'TBI')
    curr = dados_ia.get('currency', 'USD')
    incoterm = dados_ia.get('incoterm', 'TBI')
    loc = dados_ia.get('location', 'TBI')

    if vol and vol != "TBI":
        linhas.append(f"⚖️ VOL: {vol}")
    if price and price != "TBI":
        linhas.append(f"💲 PREÇO: {curr} {price}")
    if incoterm and incoterm != "TBI":
        linhas.append(f"🚢 ROTA: {incoterm} {loc}")

    comm = dados_ia.get('commission')
    if comm and comm != "TBI":
        linhas.append(f"🤝 COMISSÃO: {comm}")
    pay = dados_ia.get('payment_instrument')
    if pay:
        linhas.append(f"🏦 PAGTO: {pay}")

    return "\n".join(linhas)


class SpreadsheetSyncAgent:
    def __init__(self):
        self.drive = DriveService()
        self.pdf = PDFService()
        self.sheets_service = build('sheets', 'v4', credentials=drive_manager.creds)
        self.drive_api_v3 = build('drive', 'v3', credentials=drive_manager.creds)

        # Atlas MDM — substitui MAPEAMENTO_PRODUTOS + COMMODITIES_CORE_PERMITIDAS.
        self.resolver = ProductResolver.from_default(
            core_root_id=SAMBA_ROOT_FOLDER_ID,
            other_root_id=SAMBA_NEGOCIOS_FOLDER_ID,
        )

        # Gateway LLM — UMA falha trip o disjuntor pelo run inteiro
        # (preserva semântica do `ia_offline = True` do código anterior).
        #
        # Fast-fail explícito:
        #   - `max_attempts=1` no tenacity externo (sem retry no gateway)
        #   - `extract_quote_data(..., max_retries=1, base_sleep=0.0)` neutraliza
        #     o ciclo interno de fallback de modelos do gemini_api (que somava
        #     8+16+24+32+40+48s = ~3 min por linha quando a cota está zerada).
        # Resultado: 1ª linha tenta uma vez, se falhar abre o disjuntor e as
        # demais linhas viram PENDING_IA instantaneamente.
        def _llm_fast(**kw):
            return extract_quote_data(**kw, max_retries=1, base_sleep=0.0)

        self.gateway = LLMGateway(
            extract_fn=_llm_fast,
            breaker=CircuitBreaker(
                failure_threshold=1,
                cooldown_seconds=10**9,
            ),
            max_attempts=1,
            base_wait=0.0,
            max_wait=0.0,
        )

        self.status_writer = SheetStatusWriter(
            sheets_service=self.sheets_service,
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name=SHEET_NAME,
            status_column=STATUS_COLUMN,
            extras_column=EXTRATO_COLUMN,
        )

        self.pastas_produto_cache: dict[str, str] = {}
        self.status_batch: list[tuple[int, SyncStatus, str]] = []

    # ------------------------------------------------------------------
    # Drive — hierarquia de pastas a partir do ResolvedProduct
    # ------------------------------------------------------------------

    def obter_ou_criar_pasta_produto(self, resolved: ResolvedProduct) -> str | None:
        """Garante a hierarquia <root>/<seg1>/<seg2>... e devolve o ID da folha."""
        cache_key = resolved.canonical_path
        if cache_key in self.pastas_produto_cache:
            return self.pastas_produto_cache[cache_key]

        parent_id = resolved.root_folder_id
        for depth, segment in enumerate(resolved.folder_segments):
            seg_norm = normalize_text(segment)
            existentes = self.drive.listar_subpastas(parent_id)

            # Filtro de pastas administrativas só na raiz (preserva monolito).
            def _match(p):
                if depth == 0 and p['name'].startswith('_'):
                    return False
                return normalize_text(p['name']) == seg_norm

            match = next((p['id'] for p in existentes if _match(p)), None)
            if match:
                parent_id = match
                continue

            label = "categoria matriz" if depth == 0 else "subcategoria estrutural"
            logger.info(f"📦 Criando {label}: {segment}")
            new_id, _ = self.drive.criar_pasta_negocio(segment, parent_id=parent_id)
            parent_id = new_id

        self.pastas_produto_cache[cache_key] = parent_id
        return parent_id

    def _purge_old_fichas(self, deal_folder_id: str) -> None:
        """SEARCH & DESTROY: remove PDFs com 'FICHA'/'CADASTRO' no nome."""
        try:
            query = (
                f"'{deal_folder_id}' in parents "
                f"and mimeType='application/pdf' and trashed=false"
            )
            files = (
                self.drive_api_v3.files()
                .list(q=query, fields="files(id, name)")
                .execute()
                .get('files', [])
            )
            for pdf_file in files:
                upper = pdf_file['name'].upper()
                if "FICHA" in upper or "CADASTRO" in upper:
                    self.drive_api_v3.files().delete(fileId=pdf_file['id']).execute()
                    logger.info(f"   ♻️ Saneamento: Ficha antiga removida ({pdf_file['name']})")
        except Exception as e:
            logger.warning(f"   ⚠️ Não foi possível varrer a pasta para saneamento: {e}")

    # ------------------------------------------------------------------
    # Persistência do status na planilha
    # ------------------------------------------------------------------

    def _flush_status(self) -> None:
        if not self.status_batch:
            return
        logger.info(
            f"📝 Atualizando planilha: STATUS+EXTRATO em {len(self.status_batch)} linhas..."
        )
        try:
            self.status_writer.mark_batch_with_extras(self.status_batch)
            logger.info("✅ Planilha atualizada com sucesso.")
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar a planilha: {e}")

    # ------------------------------------------------------------------
    # Loop principal
    # ------------------------------------------------------------------

    def sincronizar_planilha_para_drive(self):
        logger.info("=========================================================")
        logger.info("⚡ INICIANDO CONCILIAÇÃO ESTRUTURAL: SHEETS -> DRIVE")
        logger.info("=========================================================")

        try:
            sheet = self.sheets_service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME
            ).execute()
            linhas = result.get('values', [])
        except Exception as e:
            logger.error(f"❌ Falha crítica ao ler a planilha: {e}")
            return

        if not linhas:
            logger.info("Planilha vazia — nada a fazer.")
            return

        logger.info(f"📊 {len(linhas)} linhas capturadas. Iniciando varredura...")

        # Mapa de status atual (coluna O) para proteção de cota.
        status_atual_por_linha: dict[int, str] = {}
        for offset, row in enumerate(linhas):
            row_idx = START_ROW + offset
            if row and len(row) > STATUS_COLUMN_INDEX:
                status_atual_por_linha[row_idx] = (
                    str(row[STATUS_COLUMN_INDEX] or "").strip().upper()
                )

        # Validação/classificação via Pydantic (rejected/skipped/actionable).
        ingestion = ingest_sheet_rows(linhas, start_row=START_ROW)

        # Linhas malformadas — visibilidade imediata pro Trading Desk.
        for r in ingestion.rejected:
            if status_atual_por_linha.get(r.row_index) == "OK":
                continue
            self.status_batch.append((r.row_index, SyncStatus.REJECTED, r.reason[:200]))

        # Linhas sem produto — espera-se que sejam linhas em branco.
        for s in ingestion.skipped:
            if status_atual_por_linha.get(s.row_index) == "OK":
                continue
            self.status_batch.append((s.row_index, SyncStatus.SKIPPED, ""))

        # Loop sobre as linhas que valem trabalho de verdade.
        for deal in ingestion.actionable:
            # Cota: linha já processada, segue.
            if status_atual_por_linha.get(deal.row_index) == "OK":
                continue

            resolved = self.resolver.resolve(deal.produto_raw)

            deal_folder_name = f"{deal.entity} - {resolved.leaf_name}"
            if deal.job_id:
                deal_folder_name = f"{deal.job_id} | {deal_folder_name}"

            logger.info(
                f"→ Processando Deal (Linha {deal.row_index}): {deal_folder_name}"
            )

            produto_folder_id = self.obter_ou_criar_pasta_produto(resolved)
            if not produto_folder_id:
                continue

            # Deal Room (1 por entidade+produto).
            pastas_existentes = self.drive.listar_subpastas(produto_folder_id)
            deal_folder_id = next(
                (
                    p['id']
                    for p in pastas_existentes
                    if normalize_text(p['name']) == normalize_text(deal_folder_name)
                ),
                None,
            )
            if not deal_folder_id:
                logger.info(f"   📂 Criando Deal Room isolado: {deal_folder_name}")
                deal_folder_id, _ = self.drive.criar_pasta_negocio(
                    deal_folder_name, parent_id=produto_folder_id
                )

            # SEARCH & DESTROY antes de regenerar — evita PDFs órfãos/quebrados.
            self._purge_old_fichas(deal_folder_id)

            # Sem contexto comercial → REJECTED, não vale gastar IA.
            if not deal.has_llm_context:
                logger.warning(f"   ❌ Rejeitado: Sem contexto na planilha.")
                self.status_batch.append(
                    (deal.row_index, SyncStatus.REJECTED, "Sem dados comerciais")
                )
                continue

            logger.info(f"   🚨 Acionando IA e formatando dados Enterprise...")
            time.sleep(5)  # respiro entre chamadas — alivia rate limit do Gemini

            try:
                dados_ia = self.gateway.extract_quote(
                    message_text=deal.free_text_for_llm,
                    sender=deal.solicitante,
                    group=deal.grupo,
                )
            except LLMUnavailable as e:
                logger.error(
                    f"   ❌ LLM indisponível ({type(e).__name__}). Cancelando Ficha."
                )
                self.status_batch.append(
                    (deal.row_index, SyncStatus.PENDING_IA, "")
                )
                continue

            extrato_bonito = formatar_extrato_enterprise(dados_ia)

            spec_final = dados_ia.get("spec", "")
            if not spec_final or "planilha" in spec_final.lower():
                spec_final = (deal.especificacao or deal.visao_rapida)[:600]

            tipo_contrato = "SPOT / EXTENSIVO"
            canon_upper = resolved.canonical_path.upper()
            if "FINANCEIRO" in canon_upper or "CPR" in canon_upper:
                tipo_contrato = "CPR / OPERAÇÃO FINANCEIRA"

            dados_consolidados = {
                "name": deal_folder_name,
                "created_at": deal.data_entrada or datetime.now(),
                "commodity": resolved.leaf_name,
                "spec": spec_final,
                "currency": dados_ia.get("currency", "USD"),
                "price": dados_ia.get("price", "TBI"),
                "volume": dados_ia.get("volume", "TBI"),
                "incoterm": dados_ia.get("incoterm", "TBI"),
                "destination": dados_ia.get("location", "TBI"),
                "contract_type": tipo_contrato,
                "commission": dados_ia.get("commission", "TBI"),
                "buyer": deal.entity,
                "source_group": deal.grupo.upper(),
            }

            segundo = int(time.time())
            temp_path = os.path.join(ROOT, "temp", f"FICHA_{segundo}.pdf")
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)

            try:
                self.pdf.gerar_ficha_pedido(dados_consolidados, temp_path)
                self.drive.upload_arquivo(temp_path, deal_folder_id)
                logger.info(f"   🚀 SUCESSO: Documento gerado.")
                self.status_batch.append(
                    (deal.row_index, SyncStatus.OK, extrato_bonito)
                )
            except Exception as e:
                logger.error(f"   ❌ Erro ao assentar o PDF no Drive: {e}")
                self.status_batch.append(
                    (deal.row_index, SyncStatus.PENDING_IA, "")
                )

        # Disparo único: STATUS (O) + EXTRATO (P) em batchUpdate.
        self._flush_status()
        logger.info("🏁 Sincronização Samba Engine concluída.")


if __name__ == "__main__":
    SpreadsheetSyncAgent().sincronizar_planilha_para_drive()
