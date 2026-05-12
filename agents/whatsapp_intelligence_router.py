"""
agents/whatsapp_intelligence_router.py
=======================================
Cascade de Inteligência para @mentions no WhatsApp Corporativo.

Filosofia fundamental: FACTS ONLY — jamais inventa, jamais alucina.

Cada nível só é acionado se o anterior não produziu resposta com confiança
suficiente. O modelo nunca recebe permissão para "deduzir" ou "estimar"
além do contexto fornecido.

Arquitetura em 5 níveis:
  L0 — Intent Parser     Gemini Flash classifica a intenção (5 categorias)
  L1 — DB Direct         SQL no SQLite — fatos brutos do sistema
  L2 — RAG Search        Busca vetorial nos documentos corporativos
  L3 — Gemini Flash      Raciocínio leve APENAS sobre o contexto recuperado
  L4 — Gemini Pro        Raciocínio profundo APENAS sobre o contexto recuperado
  L5 — Honest Fallback   "Não sei" — resposta digna sem invenção

Env vars:
  GEMINI_API_KEY          — obrigatório para L0, L3, L4
  SAMBA_MENTION_HANDLE    — palavra-chave do @mention (default: samba)

Disparo via Celery:
  task_process_mention(message_id, sender, group, question)
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import textwrap
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

logger = logging.getLogger("samba.router")

# ── Rate limiter suave (free-tier: 10 RPM) ────────────────────────────────────
# Garante máximo _GEMINI_RPM calls/min para não estourar quota free-tier.
# Em produção com billing ativo pode ser aumentado ou desativado.
_GEMINI_RPM      = int(os.getenv("GEMINI_RPM_LIMIT", "80"))  # billing ativo: ~115 RPM real
_rpm_lock        = threading.Lock()
_rpm_calls: list[float] = []   # timestamps das chamadas recentes

def _rpm_wait() -> None:
    """Bloqueia até que a janela de 1 minuto tenha menos de _GEMINI_RPM calls."""
    with _rpm_lock:
        now = time.monotonic()
        # Remove calls mais antigas que 60s
        _rpm_calls[:] = [t for t in _rpm_calls if now - t < 60.0]
        if len(_rpm_calls) >= _GEMINI_RPM:
            sleep_for = 60.0 - (now - _rpm_calls[0]) + 0.5
            logger.info("_rpm_wait: %d calls/min atingido — aguardando %.1fs", _GEMINI_RPM, sleep_for)
            time.sleep(max(sleep_for, 0))
            _rpm_calls[:] = [t for t in _rpm_calls if time.monotonic() - t < 60.0]
        _rpm_calls.append(time.monotonic())

# ── Cache de respostas Gemini (TTL 15 min) ─────────────────────────────────────
# Evita chamadas repetidas para a mesma pergunta dentro do TTL.
_response_cache: dict[str, tuple[str, float, float]] = {}  # key → (answer, conf, ts)
_CACHE_TTL = float(os.getenv("GEMINI_CACHE_TTL", "900"))   # 15 minutos

def _cache_key(model: str, prompt: str) -> str:
    return hashlib.md5(f"{model}:{prompt}".encode()).hexdigest()

def _cache_get(key: str) -> Optional[tuple[str, float]]:
    entry = _response_cache.get(key)
    if entry and (time.monotonic() - entry[2]) < _CACHE_TTL:
        logger.debug("_cache_get HIT: %s", key[:12])
        return entry[0], entry[1]
    return None

def _cache_set(key: str, answer: str, conf: float) -> None:
    _response_cache[key] = (answer, conf, time.monotonic())

# ── Configuração ─────────────────────────────────────────────────────────────

GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
MENTION_HANDLE    = os.getenv("SAMBA_MENTION_HANDLE", "samba").lower()

# Padrão liberal: @samba, @agente, @ia, @bot, @assistant (ou handle custom)
_MENTION_RE = re.compile(
    r"@(?:" + re.escape(MENTION_HANDLE) + r"|agente|ia|bot|assistant)\b",
    re.IGNORECASE,
)

# Modelos Gemini em escala de custo/capacidade (API v1 — prefixo "models/")
# Flash: rápido, barato — L0 intent + L3 raciocínio leve
# Pro:   completo, lento — L4 raciocínio profundo
MODEL_FLASH   = os.getenv("ROUTER_MODEL_FLASH", "models/gemini-2.5-flash-lite")
MODEL_PRO     = os.getenv("ROUTER_MODEL_PRO",   "models/gemini-2.5-flash")

# Confiança mínima para responder sem escalar (0–1)
CONF_RESPOND  = float(os.getenv("ROUTER_CONF_RESPOND",  "0.75"))
CONF_ESCALATE = float(os.getenv("ROUTER_CONF_ESCALATE", "0.45"))

# Tokens máximos de contexto a injetar no LLM (evita explosão de custo)
MAX_CONTEXT_CHARS = 6000

# ── Intenções reconhecidas ────────────────────────────────────────────────────

INTENTS = {
    "DEAL_LOOKUP":    "Consulta de negócio específico (JOB, parceiro, commodity, preço, status)",
    "FOLLOWUP_QUERY": "Status de follow-ups ou pendências com clientes/fornecedores",
    "MEETING_QUERY":  "Decisões, atas ou action items de reuniões",
    "PRICE_QUERY":    "Cotação de commodity ou câmbio",
    "DOCUMENT_QUERY": "Busca em contratos, manuais, procedimentos ou documentos corporativos",
    "GENERAL":        "Pergunta geral sobre a Samba Export que não se encaixa nas categorias acima",
}

# ── Estruturas de resultado ───────────────────────────────────────────────────

@dataclass
class RouterResult:
    answer:      str
    source:      str          # "db", "rag", "gemini_flash", "gemini_pro", "fallback"
    level:       int          # 1–5
    confidence:  str          # "alta", "media", "baixa"
    intent:      str = ""
    context_used: bool = False
    error:       Optional[str] = None


# ── Detecção de @mention ──────────────────────────────────────────────────────

def extract_question(body: str) -> Optional[str]:
    """
    Extrai a pergunta de uma mensagem com @mention.

    Retorna None se não houver @mention reconhecido.
    Retorna a string limpa (sem o @mention) se houver.
    """
    if not _MENTION_RE.search(body):
        return None
    question = _MENTION_RE.sub(" ", body).strip()
    # Colapsa múltiplos espaços e remove pontuação residual no início
    question = re.sub(r"\s{2,}", " ", question)
    question = re.sub(r"^[\s,;:\-]+", "", question).strip()
    return question if len(question) >= 3 else None


# ── Prompt anti-alucinação ────────────────────────────────────────────────────

_SYSTEM_FACTS_ONLY = """
Você é o Assistente de Inteligência da Samba Export, empresa de trading de commodities agrícolas.

REGRAS ABSOLUTAS — sem exceções:
1. Responda APENAS com informações do CONTEXTO fornecido abaixo.
2. Se a informação não estiver no contexto, diga exatamente: "Não encontrei essa informação no sistema."
3. NUNCA invente preços, datas, nomes, volumes ou qualquer dado numérico.
4. NUNCA use seu conhecimento geral para completar lacunas — apenas o contexto.
5. Se tiver DÚVIDA sobre qualquer dado, sinalize: "⚠️ Dado não confirmado no sistema."
6. Cite sempre a fonte (tabela ou documento) de cada informação relevante.
7. Seja direto e objetivo. Máximo 400 palavras. Formato WhatsApp (sem markdown pesado).
8. Se a pergunta for sobre preços de mercado sem dados no contexto, informe que os preços
   estão disponíveis no painel ou via cotação ao vivo — NÃO estime valores.
""".strip()

_INTENT_PROMPT = """
Classifique a intenção da pergunta abaixo em UMA das categorias:
DEAL_LOOKUP, FOLLOWUP_QUERY, MEETING_QUERY, PRICE_QUERY, DOCUMENT_QUERY, GENERAL

Responda APENAS com o nome da categoria, nada mais.

Pergunta: {question}
""".strip()


# ── Router principal ──────────────────────────────────────────────────────────

class IntelligenceRouter:
    """
    Cascade de inteligência para @mentions no WhatsApp corporativo.

    Uso:
        router = IntelligenceRouter()
        result = router.route(question="qual status do Job 2024BR001?", sender="+55...")
        formatted = router.format_whatsapp(result)
    """

    # ── L0: Classificação de intenção ─────────────────────────────────────────

    def classify_intent(self, question: str) -> str:
        """Gemini Flash classifica a intenção — barato, rápido, não retorna resposta."""
        try:
            # Cache por pergunta (intent muda pouco para o mesmo texto)
            ck = _cache_key(MODEL_FLASH, f"intent:{question}")
            cached = _cache_get(ck)
            if cached:
                return cached[0]   # answer field guarda o intent string

            _rpm_wait()
            from google import genai
            from google.genai import types as gtypes
            client = genai.Client(api_key=GEMINI_API_KEY)
            resp = client.models.generate_content(
                model=MODEL_FLASH,
                contents=_INTENT_PROMPT.format(question=question),
                config=gtypes.GenerateContentConfig(
                    max_output_tokens=20,
                    temperature=0,
                ),
            )
            intent = (resp.text or "GENERAL").strip().upper()
            intent = intent if intent in INTENTS else "GENERAL"
            _cache_set(ck, intent, 1.0)
            return intent
        except Exception as exc:
            logger.warning("classify_intent failed: %s", exc)
            return "GENERAL"

    # ── L1: Consulta direta ao banco ──────────────────────────────────────────

    def query_db(self, question: str, intent: str) -> tuple[str, bool]:
        """
        SQL direto no SQLite por intenção.

        Returns:
            (context_str, found: bool)
        """
        try:
            from models.database import get_session
            from sqlalchemy import text
            sess = get_session()
            rows_text: list[str] = []

            q_lower = question.lower()

            # ── Extrai JOB code (padrão: 4 dígitos + 2+ letras + 3+ dígitos)
            job_match = re.search(r"\b(20\d{2}[A-Z]{2,8}\d{2,6})\b", question, re.IGNORECASE)
            job_code  = job_match.group(1).upper() if job_match else None

            # ── DEAL_LOOKUP ───────────────────────────────────────────────────
            if intent in ("DEAL_LOOKUP", "GENERAL"):
                filters, params = [], {}
                if job_code:
                    filters.append("(UPPER(d.name) LIKE :job OR UPPER(d.notes) LIKE :job)")
                    params["job"] = f"%{job_code}%"

                # Tenta extrair nome de parceiro (palavras capitalizadas ≥ 4 chars)
                words = re.findall(r"\b[A-ZÀ-Ü][a-zà-ü]{3,}\b", question)
                if words:
                    for i, w in enumerate(words[:3]):
                        k = f"word{i}"
                        filters.append(
                            f"(LOWER(d.name) LIKE :{k} OR LOWER(d.source_sender) LIKE :{k} "
                            f"OR LOWER(d.commodity) LIKE :{k} OR LOWER(d.assignee) LIKE :{k})"
                        )
                        params[k] = f"%{w.lower()}%"

                # Commodities mencionadas
                commodity_map = {
                    "soja": "soja", "soy": "soja", "milho": "milho", "corn": "milho",
                    "açúcar": "acucar", "acucar": "acucar", "sugar": "acucar",
                    "farelo": "farelo", "meal": "farelo", "cacau": "cacau", "cocoa": "cacau",
                    "café": "cafe", "cafe": "cafe", "coffee": "cafe",
                    "algodão": "algodao", "algodao": "algodao", "cotton": "algodao",
                    "frango": "frango", "chicken": "frango",
                }
                for kw, comm in commodity_map.items():
                    if kw in q_lower:
                        filters.append("LOWER(d.commodity) LIKE :comm")
                        params["comm"] = f"%{comm}%"
                        break

                where = ("WHERE " + " AND ".join(filters)) if filters else ""
                sql = text(f"""
                    SELECT d.id, d.name, d.commodity, d.direcao, d.stage, d.status,
                           d.price, d.currency, d.volume, d.volume_unit, d.incoterm,
                           d.origin, d.destination, d.assignee, d.source_sender,
                           d.source_group, d.created_at, d.notes
                    FROM deals d
                    {where}
                    ORDER BY d.updated_at DESC
                    LIMIT 5
                """)
                rows = sess.execute(sql, params).fetchall()
                for r in rows:
                    rows_text.append(
                        f"[DEAL #{r.id}] {r.name} | {r.commodity} | {r.direcao} | "
                        f"Stage: {r.stage} | Status: {r.status} | "
                        f"Preço: {r.price} {r.currency}/{r.volume_unit} | "
                        f"Vol: {r.volume} {r.volume_unit} | Incoterm: {r.incoterm} | "
                        f"Origem→Destino: {r.origin}→{r.destination} | "
                        f"Responsável: {r.assignee} | Fonte: {r.source_sender} ({r.source_group}) | "
                        f"Criado: {str(r.created_at)[:10]}"
                    )

            # ── FOLLOWUP_QUERY ────────────────────────────────────────────────
            if intent in ("FOLLOWUP_QUERY", "GENERAL"):
                sql_fu = text("""
                    SELECT f.id, f.target_person, f.target_group, f.status,
                           f.due_at, f.response_received, f.response_content,
                           d.name AS deal_name, d.commodity
                    FROM followups f
                    LEFT JOIN deals d ON d.id = f.deal_id
                    WHERE f.status IN ('pendente','enviado')
                    ORDER BY f.due_at ASC
                    LIMIT 8
                """)
                fu_rows = sess.execute(sql_fu).fetchall()
                for r in fu_rows:
                    resp_flag = "✓ Respondido" if r.response_received else "⏳ Aguardando"
                    rows_text.append(
                        f"[FOLLOWUP #{r.id}] Deal: {r.deal_name} ({r.commodity}) | "
                        f"Para: {r.target_person or r.target_group} | "
                        f"Status: {r.status} | {resp_flag} | Prazo: {str(r.due_at)[:10]}"
                    )

            # ── MEETING_QUERY ─────────────────────────────────────────────────
            if intent in ("MEETING_QUERY", "GENERAL"):
                sql_mt = text("""
                    SELECT id, responsible, action, priority, status, due_date,
                           ata_snippet, source_group, meeting_date
                    FROM meeting_action_items
                    WHERE status != 'concluido'
                    ORDER BY
                        CASE priority WHEN 'critica' THEN 0
                                      WHEN 'alta'    THEN 1
                                      WHEN 'media'   THEN 2
                                      ELSE 3 END,
                        meeting_date DESC
                    LIMIT 8
                """)
                mt_rows = sess.execute(sql_mt).fetchall()
                for r in mt_rows:
                    rows_text.append(
                        f"[ACTION #{r.id}] [{r.priority.upper()}] {r.responsible}: {r.action} | "
                        f"Status: {r.status} | Prazo: {str(r.due_date or '')[:10]} | "
                        f"Reunião: {str(r.meeting_date or '')[:10]} ({r.source_group or ''})"
                    )

            # ── PRICE_QUERY ───────────────────────────────────────────────────
            if intent == "PRICE_QUERY":
                sql_pr = text("""
                    SELECT timestamp, usd_brl, cbot_soy_usd_mt, cbot_corn_usd_mt,
                           ice_sugar_usd_mt
                    FROM market_snapshots
                    ORDER BY timestamp DESC LIMIT 1
                """)
                pr = sess.execute(sql_pr).fetchone()
                if pr:
                    rows_text.append(
                        f"[MERCADO] Atualizado: {str(pr.timestamp)[:16]} | "
                        f"USD/BRL: {pr.usd_brl:.4f} | "
                        f"Soja CBOT: {pr.cbot_soy_usd_mt:.2f} USD/MT | "
                        f"Milho CBOT: {pr.cbot_corn_usd_mt:.2f} USD/MT | "
                        f"Açúcar ICE: {pr.ice_sugar_usd_mt:.2f} USD/MT"
                    )

                # tb_bolsas_base para commodities extras
                sql_bb = text("""
                    SELECT commodity, price_usd_mt, timestamp
                    FROM tb_bolsas_base
                    ORDER BY timestamp DESC LIMIT 10
                """)
                bb_rows = sess.execute(sql_bb).fetchall()
                seen = set()
                for r in bb_rows:
                    if r.commodity not in seen:
                        seen.add(r.commodity)
                        rows_text.append(
                            f"[BOLSA] {r.commodity}: {r.price_usd_mt:.2f} USD/MT "
                            f"({str(r.timestamp)[:10]})"
                        )

            sess.close()
            context = "\n".join(rows_text)
            return context[:MAX_CONTEXT_CHARS], bool(rows_text)

        except Exception as exc:
            logger.exception("query_db error: %s", exc)
            return "", False

    # ── L2: RAG — busca vetorial ──────────────────────────────────────────────

    def query_rag(self, question: str, top_k: int = 4) -> tuple[str, bool]:
        """
        Busca semântica na base de conhecimento corporativa (CorporateKnowledge).

        Returns:
            (context_str com chunks relevantes, found: bool)
        """
        try:
            from google import genai
            from models.database import get_session, CorporateKnowledge
            from sqlalchemy import text
            import struct

            # Gera embedding da pergunta (com rate limit)
            _rpm_wait()
            client = genai.Client(api_key=GEMINI_API_KEY)
            emb_result = client.models.embed_content(
                model="models/gemini-embedding-001",
                contents=question,
            )
            q_emb = emb_result.embeddings[0].values  # list[float] — 3072 dims
            q_bytes = struct.pack(f"{len(q_emb)}f", *q_emb)

            # Busca por similaridade cosine aproximada (SQLite — sem extensão vetorial)
            sess  = get_session()
            chunks = sess.query(CorporateKnowledge).all()

            if not chunks:
                sess.close()
                return "", False

            # Cosine similarity — suporta float32 binário (gemini-embedding-001, 3072 dims)
            # Chunks em formato legado (JSON 384 dims) são automaticamente ignorados (score 0)
            EXPECTED_BYTES = len(q_emb) * 4   # 3072 * 4 = 12288

            def cosine(a_bytes: bytes, b: list[float]) -> float:
                if not a_bytes or len(a_bytes) != EXPECTED_BYTES:
                    return 0.0   # formato incompatível — ignora (pendente re-embed)
                a    = list(struct.unpack(f"{len(b)}f", a_bytes))
                dot  = sum(x * y for x, y in zip(a, b))
                na   = sum(x * x for x in a) ** 0.5
                nb   = sum(x * x for x in b) ** 0.5
                return dot / (na * nb + 1e-9)

            scored = [
                (cosine(c.embedding, q_emb), c)
                for c in chunks
                if c.embedding
            ]
            scored.sort(key=lambda x: x[0], reverse=True)
            top = [(sim, c) for sim, c in scored[:top_k] if sim >= 0.55]

            sess.close()

            if not top:
                return "", False

            parts = []
            for sim, c in top:
                src  = c.document_name or "documento"
                text_snippet = (c.content or "")[:800]
                parts.append(
                    f"[DOC: {src} | sim={sim:.2f}]\n{text_snippet}"
                )

            context = "\n\n".join(parts)
            logger.info("query_rag: %d chunks relevantes (sim≥0.55)", len(top))
            return context[:MAX_CONTEXT_CHARS], True

        except Exception as exc:
            logger.warning("query_rag error: %s", exc)
            return "", False

    # ── L3/L4: Gemini com contexto ────────────────────────────────────────────

    # ── Memória de conversa (ConversationHistory) ─────────────────────────────

    def _load_history(self, sender: str, n: int = 4) -> str:
        """
        Carrega os últimos `n` turnos de conversa de `sender` da tabela
        conversation_history e retorna como bloco de texto formatado.

        Retorna string vazia se não há histórico ou em caso de erro.
        """
        if not sender:
            return ""
        try:
            from models.database import get_session, ConversationHistory
            sess = get_session()
            rows = (
                sess.query(ConversationHistory)
                .filter(ConversationHistory.session_id == sender)
                .order_by(ConversationHistory.timestamp.desc())
                .limit(n)
                .all()
            )
            sess.close()
            if not rows:
                return ""
            # Reverte para ordem cronológica
            rows = list(reversed(rows))
            lines = []
            for r in rows:
                role_label = "Pergunta" if r.role == "user" else "Resposta"
                ts = r.timestamp.strftime("%d/%m %H:%M") if r.timestamp else ""
                lines.append(f"[{ts}] {role_label}: {(r.content or '')[:300]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.debug("_load_history(%s) error: %s", sender, exc)
            return ""

    def _save_history(self, sender: str, question: str, answer: str) -> None:
        """
        Persiste o par (pergunta do usuário, resposta do assistente) em
        conversation_history para manter memória entre sessões.
        """
        if not sender:
            return
        try:
            from models.database import get_session, ConversationHistory
            sess = get_session()
            sess.add(ConversationHistory(
                session_id=sender, role="user", content=question[:2000],
            ))
            sess.add(ConversationHistory(
                session_id=sender, role="assistant", content=answer[:2000],
            ))
            sess.commit()
            sess.close()
        except Exception as exc:
            logger.debug("_save_history(%s) error: %s", sender, exc)

    def call_gemini(
        self,
        question:    str,
        context:     str,
        model:       str = MODEL_FLASH,
        max_tokens:  int = 600,
        history_ctx: str = "",
    ) -> tuple[str, float]:
        """
        Chama Gemini APENAS com o contexto recuperado.
        Retorna (resposta, confiança estimada 0–1).

        Confiança estimada por heurísticas linguísticas na resposta:
          - frases de incerteza  → reduz confiança
          - citação de fonte     → aumenta confiança
          - "não encontrei"      → confiança = 0 (sinaliza escalar)
        """
        if not GEMINI_API_KEY:
            return "Serviço de IA não configurado (GEMINI_API_KEY ausente).", 0.0

        history_block = ""
        if history_ctx:
            history_block = f"\n═══ HISTÓRICO RECENTE DESTA CONVERSA ═══\n{history_ctx}\n══════════════════════════════════════\n"

        prompt = f"""{_SYSTEM_FACTS_ONLY}
{history_block}
═══ CONTEXTO RECUPERADO DO SISTEMA ═══
{context if context else "[Nenhum dado encontrado no banco ou documentos para essa pergunta]"}
══════════════════════════════════════

PERGUNTA: {question}

RESPOSTA (baseada exclusivamente no contexto acima):"""

        try:
            # ── Cache hit? ────────────────────────────────────────────────────
            ck = _cache_key(model, prompt)
            cached = _cache_get(ck)
            if cached:
                logger.info("call_gemini: cache HIT — pulando chamada API")
                return cached

            # ── Rate limit (free-tier 10 RPM) ─────────────────────────────────
            _rpm_wait()

            from google import genai
            from google.genai import types as gtypes
            client = genai.Client(api_key=GEMINI_API_KEY)
            resp = client.models.generate_content(
                model=model,
                contents=prompt,
                config=gtypes.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    temperature=0.1,   # baixo para reduzir criatividade
                ),
            )
            answer = (resp.text or "").strip()

            # ── Heurística de confiança ────────────────────────────────────
            low_conf_phrases = [
                "não encontrei", "não tenho informação", "não há dados",
                "não está no contexto", "não posso confirmar", "não sei",
                "nenhuma informação", "sem dados",
            ]
            high_conf_phrases = [
                "fonte:", "tabela", "doc:", "deal #", "followup #", "action #",
                "de acordo com", "segundo o sistema",
            ]

            ans_lower = answer.lower()
            if any(p in ans_lower for p in low_conf_phrases):
                conf = 0.2
            else:
                low_hits  = sum(1 for p in ["talvez", "possivelmente", "pode ser", "provavelmente"] if p in ans_lower)
                high_hits = sum(1 for p in high_conf_phrases if p in ans_lower)
                conf = min(1.0, 0.60 + high_hits * 0.12 - low_hits * 0.10)

            # ── Persiste no cache ──────────────────────────────────────────────
            if answer:
                _cache_set(ck, answer, conf)

            return answer, conf

        except Exception as exc:
            logger.exception("call_gemini(%s) error: %s", model, exc)
            return "", 0.0

    # ── L5: Resposta honesta de fallback ──────────────────────────────────────

    @staticmethod
    def honest_fallback(question: str) -> str:
        return (
            "🤖 *SAMBA INTELLIGENCE*\n\n"
            "Consultei o banco de dados, os documentos corporativos e os modelos de IA, "
            "mas *não encontrei informação suficiente* no sistema para responder essa "
            "pergunta com precisão.\n\n"
            "Para evitar fornecer dados incorretos, prefiro não adivinhar.\n\n"
            "Sugestões:\n"
            "  • Verifique no painel: Samba Dashboard\n"
            "  • Consulte o responsável pelo negócio\n"
            "  • Envie documentos para eu indexar e poder responder na próxima vez\n\n"
            "_Samba Intelligence Engine — Facts Only_ 🎷"
        )

    # ── Roteador principal ────────────────────────────────────────────────────

    def route(
        self,
        question:   str,
        sender:     str = "",
        message_id: int = 0,
    ) -> RouterResult:
        """
        Executa o cascade completo e devolve RouterResult.

        Lógica de escalonamento:
          L1 hit         → responde com Gemini Flash como formatador (contexto DB)
          L1 miss + L2   → responde com Gemini Flash (contexto RAG)
          L1+L2 miss     → Gemini Flash sem contexto (constrained)
          conf < 0.45    → escala para Gemini Pro
          Pro conf < 0.45→ honest fallback
        """
        start = datetime.utcnow()
        logger.info("route: question='%s' sender=%s msg_id=%s", question[:80], sender, message_id)

        # ── Memória — carrega histórico recente do remetente ──────────────────
        history_ctx = self._load_history(sender, n=4)
        if history_ctx:
            logger.info("route: history loaded (%d chars) for %s", len(history_ctx), sender)

        # ── L0 — Intenção ────────────────────────────────────────────────────
        intent = self.classify_intent(question)
        logger.info("route: intent=%s", intent)

        # ── L1 — DB ──────────────────────────────────────────────────────────
        db_ctx, db_found = self.query_db(question, intent)
        logger.info("route: L1 db_found=%s ctx_len=%d", db_found, len(db_ctx))

        # ── L2 — RAG ─────────────────────────────────────────────────────────
        rag_ctx = ""
        rag_found = False
        if intent == "DOCUMENT_QUERY" or not db_found:
            rag_ctx, rag_found = self.query_rag(question)
            logger.info("route: L2 rag_found=%s ctx_len=%d", rag_found, len(rag_ctx))

        # Contexto combinado — DB tem prioridade (mais confiável)
        context_parts = []
        if db_ctx:
            context_parts.append("=== DADOS DO BANCO DE DADOS ===\n" + db_ctx)
        if rag_ctx:
            context_parts.append("=== DOCUMENTOS CORPORATIVOS ===\n" + rag_ctx)
        combined_ctx = "\n\n".join(context_parts)

        # ── L3 — Gemini Flash ─────────────────────────────────────────────────
        answer_flash, conf_flash = self.call_gemini(
            question, combined_ctx, model=MODEL_FLASH, max_tokens=500,
            history_ctx=history_ctx,
        )
        logger.info("route: L3 conf=%.2f", conf_flash)

        if conf_flash >= CONF_RESPOND:
            result = RouterResult(
                answer=answer_flash,
                source="gemini_flash",
                level=3,
                confidence="alta" if conf_flash >= 0.80 else "media",
                intent=intent,
                context_used=bool(combined_ctx),
            )
            self._save_history(sender, question, answer_flash)
            return result

        # ── L4 — Gemini Pro (escala) ──────────────────────────────────────────
        if conf_flash >= CONF_ESCALATE:
            # Contexto parcial — tenta com Pro
            answer_pro, conf_pro = self.call_gemini(
                question, combined_ctx, model=MODEL_PRO, max_tokens=800,
                history_ctx=history_ctx,
            )
            logger.info("route: L4 conf=%.2f", conf_pro)

            if conf_pro >= CONF_ESCALATE:
                result = RouterResult(
                    answer=answer_pro,
                    source="gemini_pro",
                    level=4,
                    confidence="media" if conf_pro >= CONF_RESPOND else "baixa",
                    intent=intent,
                    context_used=bool(combined_ctx),
                )
                self._save_history(sender, question, answer_pro)
                return result

        # ── L5a — Raw DB/RAG fallback (LLM indisponível mas dados encontrados) ──
        # Quando Gemini está indisponível (rate limit/erro) mas temos dados reais no
        # banco, preferimos entregar os dados brutos formatados a fingir que não há info.
        if combined_ctx:
            logger.info("route: L5a raw_db_fallback — Gemini indisponível mas DB encontrou dados")
            raw_answer = (
                "📊 *Dados encontrados no sistema* _(IA temporariamente indisponível)_\n\n"
                + combined_ctx[:2000]
                + "\n\n_Use o painel para visualização completa._"
            )
            result = RouterResult(
                answer=raw_answer,
                source="db" if db_found else "rag",
                level=5,
                confidence="media",
                intent=intent,
                context_used=True,
            )
            self._save_history(sender, question, raw_answer)
            return result

        # ── L5b — Honest fallback (nenhum dado encontrado) ────────────────────
        logger.info("route: L5b honest_fallback — nenhum nível produziu resposta confiável")
        fallback_answer = self.honest_fallback(question)
        self._save_history(sender, question, fallback_answer)
        return RouterResult(
            answer=fallback_answer,
            source="fallback",
            level=5,
            confidence="baixa",
            intent=intent,
            context_used=False,
        )

    # ── Formatação WhatsApp ───────────────────────────────────────────────────

    @staticmethod
    def format_whatsapp(result: RouterResult, question: str = "") -> str:
        """
        Formata RouterResult para mensagem WhatsApp (sem markdown pesado).

        Layout:
          🤖 SAMBA INTELLIGENCE
          [resposta]
          — Fonte | Nível | Confiança
        """
        SOURCE_LABELS = {
            "db":           "📊 Banco de dados",
            "rag":          "📁 Documentos corporativos",
            "gemini_flash": "⚡ Gemini Flash",
            "gemini_pro":   "🧠 Gemini Pro",
            "fallback":     "❓ Sem dados suficientes",
        }
        CONF_ICONS = {"alta": "🟢", "media": "🟡", "baixa": "🔴"}

        src_label  = SOURCE_LABELS.get(result.source, result.source)
        conf_icon  = CONF_ICONS.get(result.confidence, "⚪")
        conf_label = result.confidence.upper()

        # Separa o header da resposta de fallback (já formatada) das respostas normais
        if result.source == "fallback":
            return result.answer  # já tem header próprio

        header  = "🤖 *SAMBA INTELLIGENCE*\n\n"
        body    = result.answer.strip()
        footer  = (
            f"\n\n_{src_label} · {conf_icon} Confiança {conf_label}"
            + (f" · Intenção: {result.intent}" if result.intent else "")
            + "_"
        )

        # Aviso se confiança baixa
        if result.confidence == "baixa":
            footer += "\n_⚠️ Verifique esta informação no painel antes de agir._"

        return header + body + footer


# ── Singleton ─────────────────────────────────────────────────────────────────

_router_instance: Optional[IntelligenceRouter] = None

def get_router() -> IntelligenceRouter:
    """Factory singleton — reutiliza instância entre calls do mesmo worker."""
    global _router_instance
    if _router_instance is None:
        _router_instance = IntelligenceRouter()
    return _router_instance
