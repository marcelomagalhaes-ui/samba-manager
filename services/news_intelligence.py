"""
services/news_intelligence.py
==============================
Geopolitical Sentinel — monitora notícias estratégicas via NewsData.io e
cruza com as commodities do pipeline ativo para gerar alertas direcionados.

Provider: NewsData.io (https://newsdata.io)
  - Endpoint: GET https://newsdata.io/api/1/latest
  - Free tier: 200 req/dia por chave
  - Suporta: q (boolean), category, language, size (max 10 free)
  - Resposta: {status, totalResults, results:[{title,description,link,
               keywords,category,pubDate,source_id,country,language}]}

Env vars:
  NEWSDATA_KEY_1    — chave primária NewsData.io
  NEWSDATA_KEY_2    — chave de rotação (fallback quando KEY_1 esgota quota)
  NEWS_API_KEY      — chave legada NewsAPI.org (fallback se nenhuma NEWSDATA key)

Key Rotation Protocol:
  Ao receber erro code="DailyLimitReached" (HTTP 422/429), o módulo marca a
  chave corrente como esgotada e tenta a próxima da lista. Esgotamento é
  rastreado em memória por sessão do worker (sem Redis) — reset no restart.

Budget de requests com 2 chaves e schedule a cada 30 min:
  5 buckets × 48 runs/dia = 240 req → KEY_1 cobre 200 (40 runs) + KEY_2 cobre 40.

Fluxo:
  run_geopolitical_scan()
    ├── Lê commodities ativas (SQLite)
    ├── Itera QUERY_BUCKETS filtrando por relevância ao pipeline
    ├── fetch_newsdata_with_rotation() → 1 chamada API por bucket
    └── Retorna alertas ordenados por impacto (critica → alta → media)

  format_strategic_alert(alerts) → str WhatsApp (Suits Style)
  format_morning_pulse(...)      → Morning Pulse WPP
"""
from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger("samba.geopolitical")

# ── Chaves NewsData.io (rotação automática) ──────────────────────────────────

_NEWSDATA_KEYS: list[str] = [
    k for k in [
        os.getenv("NEWSDATA_KEY_1", ""),
        os.getenv("NEWSDATA_KEY_2", ""),
    ] if k
]

# Fallback legado — NewsAPI.org (mantido para compatibilidade)
_NEWSAPI_KEY: str = os.getenv("NEWS_API_KEY", "")

# ── Rastreamento de chaves esgotadas — PERSISTIDO em arquivo ────────────────
# Problema anterior: _exhausted_keys era in-memory → resetava a cada restart
# do Streamlit, fazendo todas as chaves serem tentadas novamente (esgotando quota).
# Solução: persiste em data/newsdata_exhausted.json com timestamp UTC do dia.
# Na virada da meia-noite UTC, o arquivo é ignorado e as chaves voltam.

import json as _json
from pathlib import Path as _Path

_EXHAUSTED_STATE_FILE  = _Path(__file__).resolve().parent.parent / "data" / "newsdata_exhausted.json"
_TRANSLATION_CACHE_FILE = _Path(__file__).resolve().parent.parent / "data" / "translation_cache.json"


# ── Cache de traduções — persiste entre sessões para não chamar a API duas vezes ─

def _load_translation_cache() -> dict[str, dict]:
    try:
        if _TRANSLATION_CACHE_FILE.exists():
            return _json.loads(_TRANSLATION_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_translation_cache(cache: dict) -> None:
    try:
        _TRANSLATION_CACHE_FILE.parent.mkdir(exist_ok=True)
        _TRANSLATION_CACHE_FILE.write_text(
            _json.dumps(cache, ensure_ascii=False, indent=None),
            encoding="utf-8",
        )
    except Exception:
        pass

def _text_key(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:12]


def _get_anthropic_key() -> str:
    """Lê ANTHROPIC_API_KEY do env; se vazio, tenta o .env do projeto diretamente."""
    key = os.getenv("ANTHROPIC_API_KEY", "")
    if key:
        return key
    # Fallback: lê direto do arquivo .env (evita dependência do processo ter carregado dotenv)
    env_file = _Path(__file__).resolve().parent.parent / ".env"
    try:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("ANTHROPIC_API_KEY=") and not line.startswith("#"):
                return line.split("=", 1)[1].strip()
    except Exception:
        pass
    return ""


def _translate_alerts_ptbr(alerts: list[dict]) -> list[dict]:
    """
    Traduz headline e description de cada alerta para pt-BR via Claude Haiku.
    Usa cache em arquivo — textos já traduzidos não chamam a API novamente.
    Falha silenciosamente: retorna alerta com campos originais se erro.
    """
    api_key = _get_anthropic_key()
    if not api_key:
        logger.warning("_translate_alerts_ptbr: ANTHROPIC_API_KEY não encontrada — sem tradução")
        return alerts  # sem chave → sem tradução

    cache = _load_translation_cache()
    to_translate: list[tuple[int, str, str]] = []  # (idx, hl, desc)

    for i, alert in enumerate(alerts):
        hl   = alert.get("headline", "")
        desc = alert.get("description", "")
        key  = _text_key(hl + "|" + desc)
        if key not in cache:
            to_translate.append((i, hl, desc, key))

    if to_translate:
        # Monta batch único — 1 chamada para todos os textos pendentes
        items_json = _json.dumps(
            [{"i": i, "hl": hl, "desc": desc[:150]} for i, hl, desc, _ in to_translate],
            ensure_ascii=False,
        )
        prompt = (
            "Traduza os títulos e descrições de notícias abaixo para português brasileiro.\n"
            "Mantenha nomes próprios, siglas e termos técnicos em inglês (ex: CBOT, FOB, VLSFO).\n"
            "Retorne APENAS um JSON array com objetos {\"i\", \"hl\", \"desc\"} na mesma ordem.\n\n"
            f"{items_json}"
        )
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2048,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
            # Extrai JSON da resposta (pode vir com ```json ... ```)
            if "```" in raw:
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            translated = _json.loads(raw)
            # Salva no cache
            idx_map = {item[0]: item[3] for item in to_translate}  # i → key
            for t in translated:
                key = idx_map.get(t["i"])
                if key:
                    cache[key] = {"hl": t.get("hl", ""), "desc": t.get("desc", "")}
            _save_translation_cache(cache)
        except Exception as exc:
            logger.warning("_translate_alerts_ptbr: erro na tradução — %s", exc)
            # Continua com originais

    # Aplica traduções aos alertas
    for alert in alerts:
        hl   = alert.get("headline", "")
        desc = alert.get("description", "")
        key  = _text_key(hl + "|" + desc)
        if key in cache:
            alert["headline_ptbr"]     = cache[key].get("hl", hl)
            alert["description_ptbr"]  = cache[key].get("desc", desc)
        else:
            alert["headline_ptbr"]    = hl
            alert["description_ptbr"] = desc

    return alerts

def _load_exhausted_state() -> set[str]:
    """Lê chaves esgotadas do arquivo; ignora se data != hoje UTC."""
    try:
        if not _EXHAUSTED_STATE_FILE.exists():
            return set()
        raw = _json.loads(_EXHAUSTED_STATE_FILE.read_text(encoding="utf-8"))
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if raw.get("date") != today:
            return set()  # novo dia → ignora
        return set(raw.get("keys", []))
    except Exception:
        return set()

def _save_exhausted_state(keys: set[str]) -> None:
    """Persiste chaves esgotadas com data UTC de hoje."""
    try:
        _EXHAUSTED_STATE_FILE.parent.mkdir(exist_ok=True)
        _EXHAUSTED_STATE_FILE.write_text(
            _json.dumps({"date": datetime.utcnow().strftime("%Y-%m-%d"), "keys": list(keys)},
                        ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass

# Carrega estado persistido na inicialização do módulo
_exhausted_keys: set[str] = _load_exhausted_state()

NEWSDATA_URL = "https://newsdata.io/api/1/latest"
NEWSAPI_URL  = "https://newsapi.org/v2/everything"

IMPACT_ORDER = {"critica": 0, "alta": 1, "media": 2}
IMPACT_ICON  = {"critica": "🔴", "alta": "🟠", "media": "🟡"}


# ── Matriz de Risco-Notícias (RISK-NEWS Matrix) ───────────────────────────────
# Agrupada em 5 buckets → 5 chamadas API por varredura.
# Cada bucket usa q com operadores OR para maximizar cobertura por request.
#
# Estrutura: {label, query, commodities (None=global), impact, category}

QUERY_BUCKETS: list[dict] = [
    # ── Logística crítica — rotas e gargalos globais ─────────────────────────
    # Queries curtas com termos independentes (OR simples) rendem mais resultados
    # que frases compostas exatas no free tier do NewsData.io.
    {
        "label":       "logistics_critical",
        "query":       "Red Sea OR Suez Canal OR Panama Canal OR port strike",
        # len = 52 ✓ (era 69 com frases compostas → retornava 0)
        "commodities": None,
        "impact":      "critica",
        "category":    "business",
    },
    {
        "label":       "macro_trade_policy",
        "query":       "China tariffs OR commodity export ban OR trade sanctions",
        # len = 56 ✓
        "commodities": None,
        "impact":      "alta",
        "category":    "business",
    },
    {
        "label":       "soybean",
        "query":       "soybean export OR Brazil soy OR China soybean OR CBOT soy",
        # len = 57 ✓
        "commodities": ["soja"],
        "impact":      "alta",
        "category":    "business",
    },
    {
        "label":       "sugar_icumsa",
        "query":       "sugar export OR India sugar OR ICUMSA OR sugarcane",
        # len = 50 ✓
        "commodities": ["acucar"],
        "impact":      "alta",
        "category":    "business",
    },
    {
        "label":       "multi_commodity",
        "query":       "avian flu OR corn export OR palm oil OR chicken trade",
        # len = 53 ✓
        "commodities": ["milho", "frango", "oleo", "farelo"],
        "impact":      "alta",
        "category":    "business",
    },
]


# ── Key rotation ─────────────────────────────────────────────────────────────

def _available_keys() -> list[str]:
    """Retorna chaves NewsData.io ainda não esgotadas hoje."""
    return [k for k in _NEWSDATA_KEYS if k not in _exhausted_keys]


def _mark_exhausted(key: str) -> None:
    """Marca chave como esgotada e persiste no arquivo (sobrevive a restarts)."""
    _exhausted_keys.add(key)
    _save_exhausted_state(_exhausted_keys)
    logger.warning(
        "news_intelligence: chave NewsData.io esgotada (quota) — "
        "marcada como indisponível até meia-noite UTC. keys_restantes=%d",
        len(_available_keys()),
    )


def _is_quota_error(resp_json: dict) -> bool:
    """Detecta erro de quota no payload NewsData.io."""
    if resp_json.get("status") == "error":
        code = (resp_json.get("results") or {}).get("code", "")
        return code in (
            "DailyLimitReached",
            "RateLimitReached",
            "RequestsLimitReached",
            "MaxRequests",
        )
    return False


# ── Fetch NewsData.io com rotação ────────────────────────────────────────────

def fetch_newsdata_with_rotation(
    query: str,
    category: str = "business",
    language: str = "en",
    size: int = 10,
    retry_on_quota: bool = True,
) -> list[dict]:
    """
    Busca notícias via NewsData.io com rotação automática de chaves.

    Returns:
        Lista de articles (dicts com title, description, link, pubDate, source_id).
        Retorna [] se todas as chaves estiverem esgotadas ou se a API falhar.
    """
    keys = _available_keys()
    if not keys:
        if _NEWSAPI_KEY:
            logger.debug("fetch_newsdata_with_rotation: sem chaves NewsData — fallback NewsAPI.org")
            return _fetch_newsapi_legacy(query)
        logger.warning(
            "fetch_newsdata_with_rotation: nenhuma API key disponível — skip query='%s'",
            query[:60],
        )
        return []

    for key in keys:
        result = _call_newsdata(key, query, category=category, language=language, size=size)
        if result is None:
            # Erro de quota → marcar e tentar próxima
            _mark_exhausted(key)
            continue
        return result

    # Todas as chaves esgotadas
    logger.error(
        "fetch_newsdata_with_rotation: todas as chaves NewsData.io esgotadas — "
        "sem dados para query='%s'",
        query[:60],
    )
    return []


def _call_newsdata(
    api_key: str,
    query: str,
    *,
    category: str = "business",
    language: str = "en",
    size: int = 10,
) -> Optional[list[dict]]:
    """
    Executa uma chamada à NewsData.io API.

    Returns:
        list[dict] com articles se sucesso.
        None se erro de quota (sinaliza rotação de chave).
        [] em outros erros (rede, 5xx) — não rotaciona chave.
    """
    params: dict = {
        "apikey":   api_key,
        "q":        query,
        "category": category,
        "language": language,
        "size":     size,
    }
    try:
        resp = requests.get(NEWSDATA_URL, params=params, timeout=20)

        # NewsData.io retorna 422 ou 429 para quota exceeded,
        # mas também para parâmetros inválidos (query muito longa, etc.).
        if resp.status_code in (422, 429):
            try:
                body = resp.json()
                err_code = (body.get("results") or {}).get("code", "?")
                err_msg  = (body.get("results") or {}).get("message", "?")
            except Exception:
                err_code, err_msg = "?", resp.text[:100]
            logger.warning(
                "_call_newsdata: HTTP %d key=...%s code=%s msg=%s",
                resp.status_code, api_key[-6:], err_code, err_msg,
            )
            # Só rotaciona key em erro de quota — outros erros (query inválida) não
            # NOTA: NewsData.io pode retornar vários codes diferentes para quota:
            if err_code in (
                "DailyLimitReached", "RateLimitReached",
                "RequestsLimitReached", "MaxRequests",
                "ApiLimitExceeded",   # ← code real retornado pela API (era ignorado)
                "TooManyRequests",    # variante adicional observada
            ):
                return None  # força rotação de chave
            return []  # erro de parâmetro — não rotaciona, retorna vazio

        resp.raise_for_status()
        data = resp.json()

        if _is_quota_error(data):
            logger.warning(
                "_call_newsdata: DailyLimitReached para key=...%s",
                api_key[-6:],
            )
            return None  # força rotação

        articles = data.get("results", [])
        logger.debug(
            "_call_newsdata: %d articles para query='%s' (key=...%s)",
            len(articles), query[:50], api_key[-6:],
        )
        return articles

    except requests.Timeout:
        logger.warning("_call_newsdata: timeout para query='%s'", query[:50])
        return []
    except requests.RequestException as exc:
        logger.warning("_call_newsdata: request error — %s", exc)
        return []


# ── Fallback legado — NewsAPI.org ────────────────────────────────────────────

def _fetch_newsapi_legacy(query: str, hours_back: int = 2) -> list[dict]:
    """
    Fallback para NewsAPI.org (chave NEWS_API_KEY).
    Retorna articles no formato normalizado compatível com o restante do módulo.
    """
    if not _NEWSAPI_KEY:
        return []
    from_dt = (datetime.utcnow() - timedelta(hours=hours_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        resp = requests.get(
            NEWSAPI_URL,
            params={
                "q":        query,
                "from":     from_dt,
                "sortBy":   "relevancy",
                "language": "en",
                "pageSize": 5,
                "apiKey":   _NEWSAPI_KEY,
            },
            timeout=15,
        )
        if resp.status_code == 426:
            resp = requests.get(
                NEWSAPI_URL,
                params={"q": query, "sortBy": "relevancy",
                        "language": "en", "pageSize": 5, "apiKey": _NEWSAPI_KEY},
                timeout=15,
            )
        resp.raise_for_status()
        articles = resp.json().get("articles", [])
        # Normaliza para formato NewsData.io
        return [
            {
                "title":       a.get("title", ""),
                "description": a.get("description", ""),
                "link":        a.get("url", ""),
                "pubDate":     a.get("publishedAt", ""),
                "source_id":   (a.get("source") or {}).get("name", ""),
            }
            for a in articles
        ]
    except requests.RequestException as exc:
        logger.warning("_fetch_newsapi_legacy: error — %s", exc)
        return []


# ── Pipeline commodities ─────────────────────────────────────────────────────

def get_active_pipeline_commodities() -> set[str]:
    """
    Retorna conjunto de commodities normalizadas (lowercase) com deals ativos.
    Exemplo: {"soja", "milho", "acucar", "frango"}
    """
    try:
        from models.database import get_session
        from sqlalchemy import text as _text
        sess = get_session()
        try:
            rows = sess.execute(_text(
                "SELECT DISTINCT LOWER(commodity) FROM deals "
                "WHERE status='ativo' AND commodity IS NOT NULL AND commodity != ''"
            )).fetchall()
            return {r[0] for r in rows if r[0]}
        finally:
            sess.close()
    except Exception as exc:
        logger.warning("get_active_pipeline_commodities: %s", exc)
        return set()


def _bucket_relevant(bucket: dict, active: set[str]) -> bool:
    """
    Decide se um QUERY_BUCKET é relevante para o pipeline corrente.

    Buckets globais (commodities=None) são sempre relevantes.
    Buckets específicos: se pipeline vazio (DB indisponível ou sem deals),
    roda todos os buckets — melhor ter excesso que silêncio.
    """
    commodities = bucket.get("commodities")
    if not commodities:
        return True  # global — afeta todo o pipeline
    if not active:
        return True  # pipeline desconhecido → escaneia tudo por precaução
    return any(
        kw_comm in active_comm or active_comm in kw_comm
        for kw_comm in commodities
        for active_comm in active
    )


# ── Varredura geopolítica ────────────────────────────────────────────────────

def run_geopolitical_scan(hours_back: int = 1) -> list[dict]:
    """
    Executa varredura geopolítica e retorna alertas relevantes para o pipeline.

    Args:
        hours_back: janela temporal em horas (1h padrão para schedule de 30min).

    Returns:
        Lista de dicts ordenados por impacto (critica → alta → media):
        {bucket, impact, commodities, headline, description, link,
         source, published_at}
    """
    active = get_active_pipeline_commodities()
    logger.info(
        "run_geopolitical_scan: commodities_ativas=%s hours_back=%d "
        "newsdata_keys_disponíveis=%d",
        active or "(todas)", hours_back, len(_available_keys()),
    )

    alerts:         list[dict] = []
    seen_headlines: set[str]   = set()

    for bucket in QUERY_BUCKETS:
        if not _bucket_relevant(bucket, active):
            logger.debug(
                "run_geopolitical_scan: bucket '%s' sem sobreposição com pipeline — skip",
                bucket["label"],
            )
            continue

        articles = fetch_newsdata_with_rotation(
            query    = bucket["query"],
            category = bucket.get("category", "business"),
            language = "en",
            size     = 10,
        )

        for art in articles:
            headline = (art.get("title") or "").strip()
            if not headline or headline in seen_headlines:
                continue

            # Sem filtro temporal explícito — o endpoint /latest já retorna os
            # artigos mais recentes indexados pelo NewsData.io (normalmente do
            # mesmo dia). Um filtro por horas é contraproducente: os artigos
            # chegam com até 12-16h de atraso e seriam descartados pela borda.
            pub_raw = (art.get("pubDate") or "")

            seen_headlines.add(headline)
            alerts.append({
                "bucket":       bucket["label"],
                "impact":       bucket["impact"],
                "commodities":  bucket["commodities"] or ["global"],
                "headline":     headline,
                "description":  (art.get("description") or "")[:200],
                "link":         art.get("link", "") or art.get("url", ""),
                "source":       art.get("source_id", "") or art.get("source", ""),
                "published_at": pub_raw,
            })

    # Ordena: critica → alta → media
    alerts.sort(key=lambda x: IMPACT_ORDER.get(x["impact"], 3))
    logger.info(
        "run_geopolitical_scan: %d alertas — criticas=%d altas=%d",
        len(alerts),
        sum(1 for a in alerts if a["impact"] == "critica"),
        sum(1 for a in alerts if a["impact"] == "alta"),
    )

    # Traduz para pt-BR (Haiku, cache em arquivo)
    alerts = _translate_alerts_ptbr(alerts)

    return alerts


# ── Formatação — Strategic Alert ─────────────────────────────────────────────

def format_strategic_alert(alerts: list[dict], max_per_level: int = 3) -> str:
    """
    Formata alertas em mensagem WhatsApp Suits Style para o grupo da diretoria.

    Returns "" se não houver alertas.
    """
    if not alerts:
        return ""

    by_level: dict[str, list] = {"critica": [], "alta": [], "media": []}
    for a in alerts:
        level = a["impact"]
        if level in by_level and len(by_level[level]) < max_per_level:
            by_level[level].append(a)

    lines = [
        "🌍 *SAMBA GEOPOLITICAL SENTINEL*",
        f"_{datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}_",
        "",
    ]

    level_labels = {
        "critica": "🔴 IMPACTO CRÍTICO — AÇÃO IMEDIATA",
        "alta":    "🟠 IMPACTO ALTO — MONITORAR",
        "media":   "🟡 IMPACTO MÉDIO — ATENÇÃO",
    }

    for level, label in level_labels.items():
        items = by_level.get(level, [])
        if not items:
            continue
        lines.append(f"*{label}*")
        for a in items:
            comms = a.get("commodities", ["global"])
            tag   = (
                f"[{', '.join(c.upper() for c in comms if c != 'global')}] "
                if comms and comms != ["global"] else ""
            )
            lines.append(f"• {tag}{a['headline']}")
            src  = a.get("source") or ""
            date = (a.get("published_at") or "")[:10]
            if src or date:
                lines.append(f"  _{src}{' · ' if src and date else ''}{date}_")
            if a.get("link"):
                lines.append(f"  {a['link']}")
        lines.append("")

    lines += [
        "_Samba Intelligence Engine_",
        "_Monitoramento NewsData.io · Cruzamento com pipeline ativo_",
    ]
    return "\n".join(lines)


# ── Formatação — Morning Pulse ────────────────────────────────────────────────

def format_morning_pulse(
    market_data: dict,
    ptax: float,
    critical_items: list[dict],
    geo_alerts: list[dict],
) -> str:
    """
    Formata o Morning Pulse diário para envio WPP à diretoria (07:30 BRT).

    Args:
        market_data:    {ativo: {valor, variacao}}
        ptax:           taxa USD/BRL do dia
        critical_items: action items pendentes com prioridade critica|alta
        geo_alerts:     alertas geopolíticos recentes (últimas 12h)
    """
    date_str = datetime.utcnow().strftime("%d/%m/%Y")
    lines = [
        f"🌅 *SAMBA MORNING PULSE — {date_str}*",
        "",
    ]

    # ── Mercado ───────────────────────────────────────────────────────────────
    lines.append("📈 *MERCADO*")
    key_map = [
        ("SOY_CBOT (USD/MT)",  "Soja CBOT"),
        ("SUGAR_ICE (USD/MT)", "Açúcar ICE"),
        ("CORN_CBOT (USD/MT)", "Milho CBOT"),
        ("USD/BRL",            "USD/BRL"),
    ]
    for key, label in key_map:
        info = market_data.get(key, {})
        val  = info.get("valor", 0)
        var  = info.get("variacao", 0)
        sign = "+" if var > 0 else ""
        trend = "↑" if var > 0 else ("↓" if var < 0 else "→")
        lines.append(f"  {trend} *{label}:* {val:.2f}  _{sign}{var:.2f}%_")

    lines += ["", f"  💵 *PTAX:* R$ {ptax:.4f}", ""]

    # ── Pendências críticas ───────────────────────────────────────────────────
    if critical_items:
        lines.append(f"⚠️ *PENDÊNCIAS CRÍTICAS ({len(critical_items)})*")
        for it in critical_items[:5]:
            icon = "🔴" if it.get("priority") == "critica" else "🟠"
            resp = it.get("responsible") or "?"
            act  = (it.get("action") or "")[:120]
            lines.append(f"  {icon} *{resp}:* {act}")
        lines.append("")

    # ── Geopolítica ───────────────────────────────────────────────────────────
    criticas = [a for a in geo_alerts if a["impact"] == "critica"]
    altas    = [a for a in geo_alerts if a["impact"] == "alta"]
    if criticas or altas:
        total = len(criticas) + len(altas)
        lines.append(f"🌍 *ALERTAS GEOPOLÍTICOS ({total} relevantes)*")
        for a in criticas[:2]:
            lines.append(f"  🔴 {a['headline'][:100]}")
        for a in altas[:2]:
            lines.append(f"  🟠 {a['headline'][:100]}")
        lines.append("")

    lines.append("_Samba Intelligence Engine · Bom dia!_ 🎷")
    return "\n".join(lines)
