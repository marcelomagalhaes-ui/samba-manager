"""
services/drive_ingestion.py
============================
Motor de Ingestão Orgânica de Conhecimento — Sprint I.

Responsabilidade única: transformar qualquer PDF/DOCX/TXT/GDoc largado na pasta
COMERCIAL do Google Drive em chunks vetorizados na tabela `CorporateKnowledge`,
com idempotência garantida por hash do ficheiro.

Suporta dois modos de operação:

  run_delta_scan()   → usa Drive Changes API (StartPageToken).
                       Chamado pelo webhook /webhook/drive ou pela task Celery
                       logo após receber notificação push. Latência: segundos.

  run_full_scan()    → lista toda a pasta COMERCIAL e reprocessa ficheiros
                       cujo hash mudou. Chamado pelo Beat a cada 60 min como
                       safety-net para eventuais falhas de webhook.

Idempotência:
  Cada ficheiro processado tem seu `md5Checksum` (ou `modifiedTime` para Google Docs)
  salvo em `drive_sync_state` (key=`file_hash:{file_id}`).
  Só é reingerido se o hash mudou → mesmo arquivo largado duas vezes = zero duplicata.

Modelo de embedding:
  Estritamente `paraphrase-multilingual-MiniLM-L12-v2` (Sprint H).
  Lazy-loaded via singleton (_EMBEDDER) para não travar o worker na inicialização.

Upsert strategy:
  Ao reingerir um ficheiro alterado: DELETE todos os chunks com `document_name`
  igual ao nome do ficheiro, depois INSERT os novos. Garante consistência mesmo
  que o número de chunks mude entre versões.

Variáveis de ambiente:
  DRIVE_COMERCIAL_FOLDER_ID   — ID da pasta COMERCIAL no Drive (obrigatório para
                                 full_scan; delta_scan usa Changes API global filtrado)
  DRIVE_WEBHOOK_TOKEN         — Token compartilhado registrado no webhook do Drive
                                 (validado no endpoint /webhook/drive)
"""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime
from typing import Any

import tiktoken

logger = logging.getLogger("samba.drive_ingestion")

# ── Configuração do motor ──────────────────────────────────────────────────────

MODEL_NAME  = "paraphrase-multilingual-MiniLM-L12-v2"  # deve coincidir com rag_search.py
CHUNK_SIZE  = 800    # tokens por chunk
OVERLAP     = 150    # tokens de sobreposição entre chunks consecutivos
MIN_CHUNK_TOKENS = 10  # chunks menores são descartados (ruído de formatação)

# MIME types suportados para extração de texto.
SUPPORTED_MIMES = {
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # .docx
    "text/plain",
    "application/vnd.google-apps.document",   # Google Docs
}

_EMBEDDER = None      # singleton lazy-loaded
_TOKENIZER = None     # tiktoken singleton


# ── Singletons ─────────────────────────────────────────────────────────────────

def _get_embedder():
    global _EMBEDDER
    if _EMBEDDER is None:
        import warnings
        warnings.filterwarnings("ignore")
        from sentence_transformers import SentenceTransformer
        logger.info("drive_ingestion: carregando modelo %s...", MODEL_NAME)
        _EMBEDDER = SentenceTransformer(MODEL_NAME)
        logger.info("drive_ingestion: modelo carregado (dim=%d)", _EMBEDDER.get_sentence_embedding_dimension())
    return _EMBEDDER


def _get_tokenizer():
    global _TOKENIZER
    if _TOKENIZER is None:
        _TOKENIZER = tiktoken.get_encoding("cl100k_base")
    return _TOKENIZER


# ── State management (DriveSyncState) ─────────────────────────────────────────

def _get_state(session, key: str) -> str | None:
    from models.database import DriveSyncState
    row = session.query(DriveSyncState).filter_by(key=key).first()
    return row.value if row else None


def _set_state(session, key: str, value: str) -> None:
    from models.database import DriveSyncState
    row = session.query(DriveSyncState).filter_by(key=key).first()
    if row:
        row.value = value
        row.updated_at = datetime.utcnow()
    else:
        session.add(DriveSyncState(key=key, value=value, updated_at=datetime.utcnow()))
    # Não commitamos aqui — o chamador controla a transação.


def _file_hash_key(file_id: str) -> str:
    return f"file_hash:{file_id}"


def _needs_processing(session, file_id: str, current_hash: str) -> bool:
    """Retorna True se o ficheiro é novo ou teve seu hash alterado."""
    saved = _get_state(session, _file_hash_key(file_id))
    return saved != current_hash


def _mark_processed(session, file_id: str, current_hash: str) -> None:
    _set_state(session, _file_hash_key(file_id), current_hash)


# ── Extração de texto ──────────────────────────────────────────────────────────

def _clean_text(text: str) -> str:
    """Remove ruído típico de conversão PDF/DOCX sem destruir o conteúdo."""
    # Remove linhas só com caracteres de controle / espaços
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln and not re.match(r'^[\s\x00-\x1f]+$', ln)]
    text = "\n".join(lines)
    # Colapsa múltiplos espaços/linhas em branco excessivas
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
    return text.strip()


def _extract_pdf(data: bytes) -> str:
    """Extrai texto de bytes PDF usando pdfminer.six."""
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(io.BytesIO(data))
        return _clean_text(text or "")
    except Exception as exc:
        logger.warning("drive_ingestion: pdfminer falhou: %s", exc)
        return ""


def _extract_docx(data: bytes) -> str:
    """Extrai texto de bytes DOCX usando python-docx."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(data))
        paragraphs = [p.text.strip() for p in doc.paragraphs if p.text.strip()]
        return _clean_text("\n".join(paragraphs))
    except Exception as exc:
        logger.warning("drive_ingestion: python-docx falhou: %s", exc)
        return ""


def _extract_txt(data: bytes) -> str:
    """Decodifica texto simples tentando UTF-8 → latin-1 → ascii."""
    for enc in ("utf-8", "latin-1", "ascii"):
        try:
            return _clean_text(data.decode(enc))
        except UnicodeDecodeError:
            continue
    return ""


def _download_and_extract(drive_service, file_id: str, mime_type: str) -> str:
    """
    Faz download do ficheiro via Drive API e extrai o texto.

    Google Docs são exportados como text/plain (sem download binário).
    Demais formatos usam get_media + extractor específico.
    """
    from googleapiclient.http import MediaIoBaseDownload

    try:
        if mime_type == "application/vnd.google-apps.document":
            # Google Doc → exportar como plain text
            request = drive_service.files().export_media(fileId=file_id, mimeType="text/plain")
        else:
            request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

        data = fh.getvalue()

        if mime_type == "application/pdf":
            return _extract_pdf(data)
        elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            return _extract_docx(data)
        elif mime_type == "application/vnd.google-apps.document":
            return _clean_text(data.decode("utf-8", errors="replace"))
        else:
            return _extract_txt(data)

    except Exception as exc:
        logger.exception("drive_ingestion: falha no download/extração file_id=%s: %s", file_id, exc)
        return ""


# ── Chunking semântico ─────────────────────────────────────────────────────────

def _chunk_text(text: str) -> list[dict[str, Any]]:
    """
    Fatia o texto em janelas de CHUNK_SIZE tokens com OVERLAP de sobreposição.

    Retorna lista de {"text": str, "token_count": int}.
    Chunks abaixo de MIN_CHUNK_TOKENS são descartados (geralmente rodapés/ruído).
    """
    tokenizer = _get_tokenizer()
    tokens = tokenizer.encode(text)
    chunks: list[dict[str, Any]] = []

    i = 0
    while i < len(tokens):
        chunk_tokens = tokens[i : i + CHUNK_SIZE]
        chunk_text = tokenizer.decode(chunk_tokens)
        n = len(chunk_tokens)
        if n >= MIN_CHUNK_TOKENS:
            chunks.append({"text": chunk_text, "token_count": n})
        i += CHUNK_SIZE - OVERLAP

    return chunks


# ── Embeddings ─────────────────────────────────────────────────────────────────

def _embed_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Adiciona campo 'embedding' (JSON str) a cada chunk. In-place + retorna."""
    if not chunks:
        return chunks
    embedder = _get_embedder()
    texts = [c["text"] for c in chunks]
    vectors = embedder.encode(texts, batch_size=32, show_progress_bar=False)
    for chunk, vec in zip(chunks, vectors):
        chunk["embedding"] = json.dumps(vec.tolist())
    return chunks


# ── Upsert no CorporateKnowledge ───────────────────────────────────────────────

def _upsert_knowledge(session, document_name: str, chunks: list[dict[str, Any]]) -> int:
    """
    Substitui os chunks do documento por novos.

    DELETE todos os chunks existentes com `document_name` igual ao argumento,
    depois INSERT os novos. Retorna o número de chunks inseridos.

    Estratégia "replace-all" garante consistência mesmo quando o número de
    chunks muda entre versões do documento (truncamentos, ampliações).
    """
    from models.database import CorporateKnowledge

    deleted = (
        session.query(CorporateKnowledge)
        .filter(CorporateKnowledge.document_name == document_name)
        .delete(synchronize_session=False)
    )
    if deleted:
        logger.info("drive_ingestion: %d chunks antigos removidos para '%s'", deleted, document_name)

    for idx, chunk in enumerate(chunks):
        session.add(CorporateKnowledge(
            document_name=document_name,
            chunk_index=idx,
            content=chunk["text"],
            embedding=chunk.get("embedding"),
            token_count=chunk["token_count"],
        ))

    logger.info(
        "drive_ingestion: %d chunks inseridos para '%s'",
        len(chunks), document_name,
    )
    return len(chunks)


# ── Pipeline por arquivo ───────────────────────────────────────────────────────

def process_file(drive_service, session, file_meta: dict[str, Any]) -> dict[str, Any]:
    """
    Pipeline completo para um único ficheiro Drive:
      download → extract → chunk → embed → upsert → mark_hash

    Args:
        drive_service: instância autenticada da Drive API v3
        session:       SQLAlchemy Session (transação controlada pelo chamador)
        file_meta:     dict com 'id', 'name', 'mimeType', 'md5Checksum'/'modifiedTime'

    Returns:
        {"file": name, "status": "ingested"|"skipped"|"error", "chunks": int}
    """
    file_id   = file_meta["id"]
    file_name = file_meta.get("name", file_id)
    mime_type = file_meta.get("mimeType", "")

    # Hash canônico: md5 para binários, modifiedTime para Google Docs
    current_hash = file_meta.get("md5Checksum") or file_meta.get("modifiedTime", "")

    if mime_type not in SUPPORTED_MIMES:
        logger.debug("drive_ingestion: formato não suportado '%s' — ignorando", mime_type)
        return {"file": file_name, "status": "skipped_mime", "chunks": 0}

    if current_hash and not _needs_processing(session, file_id, current_hash):
        logger.info("drive_ingestion: '%s' sem alterações (hash idêntico) — skipped", file_name)
        return {"file": file_name, "status": "skipped_hash", "chunks": 0}

    logger.info("drive_ingestion: processando '%s' (mime=%s)", file_name, mime_type)

    text = _download_and_extract(drive_service, file_id, mime_type)
    if not text:
        logger.warning("drive_ingestion: extração vazia para '%s' — abortando", file_name)
        return {"file": file_name, "status": "error_empty", "chunks": 0}

    chunks = _chunk_text(text)
    if not chunks:
        logger.warning("drive_ingestion: nenhum chunk gerado para '%s'", file_name)
        return {"file": file_name, "status": "error_no_chunks", "chunks": 0}

    chunks = _embed_chunks(chunks)
    n = _upsert_knowledge(session, file_name, chunks)

    if current_hash:
        _mark_processed(session, file_id, current_hash)

    return {"file": file_name, "status": "ingested", "chunks": n}


# ── Delta scan (Changes API) ───────────────────────────────────────────────────

def _get_or_init_page_token(drive_service, session) -> str:
    """
    Retorna o StartPageToken salvo ou inicializa chamando getStartPageToken().

    Na primeira execução, o token representa "agora" — ficheiros existentes
    antes da ativação do webhook NÃO são processados pelo delta_scan (o
    full_scan do Beat cobre esse bootstrap).
    """
    saved = _get_state(session, "changes_page_token")
    if saved:
        return saved

    resp  = drive_service.changes().getStartPageToken(supportsAllDrives=True).execute()
    token = resp.get("startPageToken", "")
    _set_state(session, "changes_page_token", token)
    session.commit()
    logger.info("drive_ingestion: StartPageToken inicializado = %s", token)
    return token


def run_delta_scan() -> dict[str, Any]:
    """
    Processa apenas os ficheiros alterados desde o último token salvo.

    Chamado: webhook /webhook/drive (evento push do Google) ou task Celery.

    Returns:
        {"processed": int, "skipped": int, "errors": int, "new_token": str}
    """
    from models.database import get_session
    from services.google_drive import drive_manager

    if not drive_manager.service:
        logger.error("drive_ingestion: Drive não autenticado — run_delta_scan abortado")
        return {"processed": 0, "skipped": 0, "errors": 0, "new_token": ""}

    session = get_session()
    totals = {"processed": 0, "skipped": 0, "errors": 0, "new_token": ""}

    try:
        token  = _get_or_init_page_token(drive_manager.service, session)
        folder = os.getenv("DRIVE_COMERCIAL_FOLDER_ID", "")

        while True:
            resp = drive_manager.service.changes().list(
                pageToken=token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                fields="nextPageToken,newStartPageToken,changes(file(id,name,mimeType,md5Checksum,modifiedTime,parents,trashed))",
                pageSize=50,
            ).execute()

            for change in resp.get("changes", []):
                file_data = change.get("file")
                if not file_data or file_data.get("trashed"):
                    continue

                # Filtra pela pasta COMERCIAL se configurada
                if folder:
                    parents = file_data.get("parents", [])
                    if folder not in parents:
                        continue

                result = process_file(drive_manager.service, session, file_data)
                session.commit()

                st = result["status"]
                if st == "ingested":
                    totals["processed"] += 1
                elif st.startswith("error"):
                    totals["errors"] += 1
                else:
                    totals["skipped"] += 1

            new_token = resp.get("newStartPageToken")
            if new_token:
                _set_state(session, "changes_page_token", new_token)
                session.commit()
                totals["new_token"] = new_token
                break  # Última página — terminamos

            next_page = resp.get("nextPageToken")
            if next_page:
                token = next_page
            else:
                break

    except Exception:
        session.rollback()
        logger.exception("drive_ingestion: erro em run_delta_scan")
        totals["errors"] += 1
    finally:
        session.close()

    logger.info("drive_ingestion: delta_scan concluído %s", totals)
    return totals


# ── Full scan (folder listing) ─────────────────────────────────────────────────

def run_full_scan(folder_id: str | None = None) -> dict[str, Any]:
    """
    Varre a pasta COMERCIAL inteira, reingerindo ficheiros cujo hash mudou.

    Chamado: Beat a cada 60 min (safety-net contra falhas de webhook).
    Também serve como bootstrap inicial antes do webhook estar registrado.

    Args:
        folder_id: ID da pasta Drive a varrer.
                   Padrão: env DRIVE_COMERCIAL_FOLDER_ID ou SAMBA_ROOT_FOLDER_ID.

    Returns:
        {"processed": int, "skipped": int, "errors": int, "total_listed": int}
    """
    from models.database import get_session
    from services.google_drive import drive_manager, SAMBA_ROOT_FOLDER_ID

    if not drive_manager.service:
        logger.error("drive_ingestion: Drive não autenticado — run_full_scan abortado")
        return {"processed": 0, "skipped": 0, "errors": 0, "total_listed": 0}

    target_folder = (
        folder_id
        or os.getenv("DRIVE_COMERCIAL_FOLDER_ID")
        or SAMBA_ROOT_FOLDER_ID
    )

    logger.info("drive_ingestion: full_scan iniciado na pasta '%s'", target_folder)
    session = get_session()
    totals = {"processed": 0, "skipped": 0, "errors": 0, "total_listed": 0}

    try:
        page_token: str | None = None

        while True:
            # Mimes suportados em query para reduzir tráfego
            mime_filter = " or ".join(
                f"mimeType='{m}'" for m in SUPPORTED_MIMES
            )
            query = (
                f"'{target_folder}' in parents and trashed = false "
                f"and ({mime_filter})"
            )
            params: dict[str, Any] = dict(
                q=query,
                pageSize=50,
                fields="nextPageToken,files(id,name,mimeType,md5Checksum,modifiedTime)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            if page_token:
                params["pageToken"] = page_token

            resp  = drive_manager.service.files().list(**params).execute()
            files = resp.get("files", [])
            totals["total_listed"] += len(files)

            for file_meta in files:
                result = process_file(drive_manager.service, session, file_meta)
                session.commit()

                st = result["status"]
                if st == "ingested":
                    totals["processed"] += 1
                elif st.startswith("error"):
                    totals["errors"] += 1
                else:
                    totals["skipped"] += 1

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    except Exception:
        session.rollback()
        logger.exception("drive_ingestion: erro em run_full_scan")
        totals["errors"] += 1
    finally:
        session.close()

    logger.info("drive_ingestion: full_scan concluído %s", totals)
    return totals
