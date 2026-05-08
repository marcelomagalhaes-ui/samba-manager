"""
scripts/build_embeddings.py
============================
Sprint F — Popula embeddings NULL na tabela CorporateKnowledge.

Usa o mesmo modelo `all-MiniLM-L6-v2` já utilizado pelos ~225 chunks existentes,
garantindo consistência no espaço vetorial para a busca por similaridade de cosseno.

Execução:
    python scripts/build_embeddings.py [--all]

    --all  : reconstrói TODOS os embeddings (225 existentes + 22 nulls).
             Use quando trocar de modelo; exige reingestão completa.
    (padrão): preenche apenas os 22 registros com embedding IS NULL.

Saída esperada (modo padrão):
    Modelo carregado: all-MiniLM-L6-v2
    Chunks sem embedding: 22
    [1/22] id=226 20260419_SE_Soybean_Mexico.pdf#0 (206 tok)  ✓
    ...
    [22/22] id=247 TABELA_REFERENCIA_CIF_SOJA_GMO_ABR2026.internal#0 ✓
    Embeddings gerados e persistidos: 22
    Total chunks com embedding na tabela: 247
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_embeddings")

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"   # PT/EN mixed content — Sprint H


def main(rebuild_all: bool = False) -> None:
    # ── 1. Carrega o modelo ────────────────────────────────────────────────────
    try:
        import warnings
        warnings.filterwarnings("ignore")
        from sentence_transformers import SentenceTransformer
    except ImportError:
        logger.error("sentence-transformers não instalado. Rode: pip install sentence-transformers")
        sys.exit(1)

    logger.info("Carregando modelo %s (download automático na 1ª vez)...", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME)
    logger.info("Modelo carregado: %s  (dim=%d)", MODEL_NAME, model.get_sentence_embedding_dimension())

    # ── 2. Consulta os registros alvo ──────────────────────────────────────────
    from models.database import get_session, CorporateKnowledge

    session = get_session()
    try:
        if rebuild_all:
            targets = session.query(CorporateKnowledge).all()
            logger.info("Modo --all: %d chunks no total serão (re)computados.", len(targets))
        else:
            targets = (
                session.query(CorporateKnowledge)
                .filter(CorporateKnowledge.embedding == None)   # noqa: E711
                .all()
            )
            logger.info("Chunks sem embedding: %d", len(targets))

        if not targets:
            logger.info("Nada a fazer — todos os chunks já têm embedding.")
            return

        # ── 3. Gera e persiste embeddings em batch ─────────────────────────────
        texts  = [item.content for item in targets]
        logger.info("Codificando %d textos em batch...", len(texts))
        vectors = model.encode(texts, batch_size=32, show_progress_bar=True, normalize_embeddings=False)

        for idx, (item, vec) in enumerate(zip(targets, vectors), 1):
            item.embedding = json.dumps(vec.tolist())
            logger.info(
                "[%d/%d] id=%-4d  %-45s chunk=%-2d  dim=%d  ✓",
                idx, len(targets),
                item.id,
                (item.document_name or "")[:45],
                item.chunk_index or 0,
                len(vec),
            )

        session.commit()
        logger.info("Embeddings gerados e persistidos: %d", len(targets))

        # ── 4. Relatório final ─────────────────────────────────────────────────
        total   = session.query(CorporateKnowledge).count()
        n_null  = session.query(CorporateKnowledge).filter(CorporateKnowledge.embedding == None).count()  # noqa: E711
        logger.info("Total chunks na tabela: %d  |  Ainda sem embedding: %d", total, n_null)

    except Exception:
        session.rollback()
        logger.exception("Erro ao gerar embeddings — rollback executado.")
        sys.exit(1)
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Constrói embeddings para CorporateKnowledge.")
    parser.add_argument(
        "--all", dest="rebuild_all", action="store_true",
        help="Reconstrói TODOS os embeddings (use ao trocar de modelo).",
    )
    args = parser.parse_args()
    main(rebuild_all=args.rebuild_all)
