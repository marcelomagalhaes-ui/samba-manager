"""
agents/training_agent.py
========================
Agente de Treinamento Samba Export — chatbot de capacitação da equipe comercial.

Persona: Samba Export (empresa, não um sócio específico).
Salva dúvidas relevantes na tabela faq_questions do banco SQLite.
"""
from __future__ import annotations

import datetime
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """Você é o assistente de treinamento da Samba Export, uma trading company
brasileira especializada na exportação de commodities agrícolas (soja, milho, açúcar, proteínas animais).

Sua personalidade:
- Profissional, direto e objetivo
- Fala em nome da Samba Export (não de um sócio específico)
- Especialista em: incoterms, documentos de exportação, logística marítima, prospecção de compradores
- Conhece bem o mercado de açúcar ICUMSA, soja e milho
- Explica conceitos com exemplos práticos do dia a dia da exportação

Ao responder:
1. Responda de forma clara e estruturada
2. Use exemplos reais quando possível
3. Se mencionar documentos (BL, COA, SPA, etc.), explique brevemente cada um
4. Se a pergunta for sobre preço ou negociação, sempre mencione incoterm relevante
5. Seja conciso — a equipe está ocupada

Contexto: Você treina a equipe de vendas/prospecção da Samba Export.
"""

_QUICK_QUESTIONS = [
    "O que é CIF e FOB?",
    "Lead quer comprar açúcar ICUMSA 45 vs 150",
    "Lead não respondeu",
    "O que é Finder?",
    "Lead pediu preço de milho",
    "O que é NCDA?",
    "Como criar grupo WhatsApp",
]


def get_quick_questions() -> list[str]:
    """Retorna lista de perguntas rápidas sugeridas."""
    return _QUICK_QUESTIONS


def chat_training(
    messages: list[dict],
    model: str = "claude-haiku-4-5",
) -> str:
    """
    Envia histórico de mensagens ao Claude e retorna a resposta.

    Args:
        messages: Lista de dicts {"role": "user"|"assistant", "content": str}
        model:    Modelo Claude a usar

    Returns:
        Texto da resposta do assistente.
    """
    try:
        import anthropic
        client = anthropic.Anthropic(timeout=60.0)   # 60s — chat pode ser mais longo
        response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=_SYSTEM_PROMPT,
            messages=messages,
        )
        return response.content[0].text.strip()
    except Exception as exc:
        logger.exception("chat_training falhou")
        return f"Erro ao processar resposta: {exc}"


def create_faq_table(engine=None) -> bool:
    """Cria a tabela faq_questions se não existir."""
    try:
        from models.database import get_engine
        import sqlalchemy as sa
        eng = engine or get_engine()
        with eng.connect() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS faq_questions (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_code  TEXT    DEFAULT '',
                    question   TEXT    NOT NULL,
                    answer     TEXT    NOT NULL,
                    tags       TEXT    DEFAULT '',
                    created_at TEXT    DEFAULT ''
                )
            """))
            conn.commit()
        return True
    except Exception as exc:
        logger.warning("create_faq_table falhou: %s", exc)
        return False


def save_faq(
    user_code: str,
    question: str,
    answer: str,
    tags: str = "",
    db_session=None,
) -> Optional[int]:
    """
    Salva uma pergunta/resposta na tabela faq_questions.

    Returns:
        ID do registro ou None se falhar.
    """
    try:
        from models.database import get_session
        import sqlalchemy as sa
        sess = db_session or get_session()
        result = sess.execute(
            sa.text("""
                INSERT INTO faq_questions (user_code, question, answer, tags, created_at)
                VALUES (:user_code, :question, :answer, :tags, :created_at)
            """),
            {
                "user_code":  user_code,
                "question":   question,
                "answer":     answer,
                "tags":       tags,
                "created_at": datetime.datetime.now().isoformat(),
            }
        )
        sess.commit()
        return result.lastrowid
    except Exception as exc:
        logger.warning("save_faq falhou: %s", exc)
        return None


def load_faqs(limit: int = 200, db_session=None) -> list[dict]:
    """Carrega FAQs do banco de dados, mais recentes primeiro."""
    try:
        from models.database import get_session
        import sqlalchemy as sa
        sess = db_session or get_session()
        rows = sess.execute(
            sa.text("""
                SELECT id, user_code, question, answer, tags, created_at
                FROM faq_questions
                ORDER BY id DESC
                LIMIT :lim
            """),
            {"lim": limit}
        ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.warning("load_faqs falhou: %s", exc)
        return []


def delete_faq(faq_id: int, db_session=None) -> bool:
    """Remove um registro da tabela faq_questions pelo ID."""
    try:
        from models.database import get_session
        import sqlalchemy as sa
        sess = db_session or get_session()
        sess.execute(
            sa.text("DELETE FROM faq_questions WHERE id = :id"),
            {"id": faq_id}
        )
        sess.commit()
        return True
    except Exception as exc:
        logger.warning("delete_faq falhou: %s", exc)
        return False
