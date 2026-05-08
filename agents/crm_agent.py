"""
agents/crm_agent.py
===================
Agente de extração CRM — converte mensagens não estruturadas
(WhatsApp, e-mail, LinkedIn, texto livre) em registros CRM padronizados.

Campos extraídos:
  nome, empresa, commodity, volume, porto_destino, pais_destino,
  status_lead, observacoes, fonte, confianca

Uso:
    from agents.crm_agent import extract_crm
    record = extract_crm("Oi, sou João da ABC Foods, interesse em 5000 MT de soja CIF Shanghai")
"""
from __future__ import annotations

import json
import logging
import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── Campos CRM padrão Samba Export ────────────────────────────────────────────
_CRM_FIELDS = {
    "nome":           "Nome completo do contato (string, vazio se não identificado)",
    "empresa":        "Nome da empresa / organização do contato",
    "commodity":      "Commodity de interesse: Soja, Milho, Açúcar, Frango, Suíno, Milho, etc.",
    "volume":         "Volume em MT (toneladas métricas) — número ou string '5000 MT'",
    "porto_destino":  "Porto de destino principal mencionado",
    "pais_destino":   "País de destino",
    "incoterm":       "Incoterm mencionado: CIF, FOB, CFR, etc. (vazio se não mencionado)",
    "status_lead":    "Um de: Novo | Em Negociação | Qualificado | Perdido | Aguardando",
    "observacoes":    "Contexto adicional relevante extraído da mensagem",
    "fonte":          "Canal de origem: WhatsApp | E-mail | LinkedIn | Outro",
    "confianca":      "Nível de confiança da extração: Alto | Médio | Baixo",
}

_SYSTEM_PROMPT = """Você é um agente de CRM especialista em commodities de exportação agrícola.
Sua tarefa é extrair informações estruturadas de mensagens comerciais não estruturadas
(WhatsApp, e-mail, LinkedIn, texto livre) e formatá-las como um registro CRM.

Regras:
- Extraia APENAS o que está explícito ou fortemente implícito no texto
- Para campos não identificáveis, use string vazia ""
- Volume deve ser em MT (toneladas métricas) quando possível
- Status padrão para novas mensagens é "Novo" a menos que haja indicação clara
- Responda APENAS com JSON válido, sem markdown, sem explicação
- IMPORTANTE: se a mensagem contiver MÚLTIPLAS commodities (ex: Soja E Milho),
  retorne um ARRAY JSON com um objeto por commodity — cada um com seu próprio
  volume, garantia, pagamento, etc. extraídos do bloco correspondente.
  Se for apenas uma commodity, retorne um array com 1 objeto.
  Formato sempre: [ { campos... }, { campos... } ]
- Para porto_destino: extraia o porto SE mencionado explicitamente.
  Se apenas o país/destino for mencionado (ex: "CIF Turquia", "CIF China"),
  coloque o principal porto importador de commodities desse país.
  Exemplos: Turquia→Mersin | China→Main Port China | Índia→Mundra |
  Paquistão→Karachi | Bangladesh→Chittagong | Vietnam→Ho Chi Minh |
  Indonésia→Tanjung Priok | Malásia→Port Klang | Arábia Saudita→Jeddah |
  UAE→Jebel Ali | Egito→Alexandria | Marrocos→Casablanca | Irã→Bandar Abbas |
  Iraque→Umm Qasr | Omã→Salalah | Argélia→Algiers | México→Veracruz |
  Colômbia→Cartagena | Peru→Callao | Chile→San Antonio
"""

# ── Lookup de portos por país — fallback pós-extração ─────────────────────────
# Usado quando o modelo não preenche porto_destino mas preenche pais_destino.
# Cobre os principais mercados de commodities agrícolas da Samba Export.
_PORTO_FALLBACK: dict[str, str] = {
    # Oriente Médio
    "turquia":         "Mersin",
    "turkey":          "Mersin",
    "arabia saudita":  "Jeddah",
    "saudi arabia":    "Jeddah",
    "uae":             "Jebel Ali",
    "emirados arabes": "Jebel Ali",
    "emirados":        "Jebel Ali",
    "oman":            "Salalah",
    "omã":             "Salalah",
    "ira":             "Bandar Abbas",
    "iran":            "Bandar Abbas",
    "iraque":          "Umm Qasr",
    "iraq":            "Umm Qasr",
    # Ásia
    "china":           "Main Port China",
    "india":           "Mundra",
    "índia":           "Mundra",
    "paquistao":       "Karachi",
    "pakistan":        "Karachi",
    "bangladesh":      "Chittagong",
    "vietnam":         "Ho Chi Minh",
    "vietna":          "Ho Chi Minh",
    "indonesia":       "Tanjung Priok",
    "indonésia":       "Tanjung Priok",
    "malasia":         "Port Klang",
    "malásia":         "Port Klang",
    "malaysia":        "Port Klang",
    "filipinas":       "Manila",
    "philippines":     "Manila",
    "coreia":          "Busan",
    "korea":           "Busan",
    "japao":           "Tokyo/Yokohama",
    "japão":           "Tokyo/Yokohama",
    "japan":           "Tokyo/Yokohama",
    # África
    "egito":           "Alexandria",
    "egypt":           "Alexandria",
    "marrocos":        "Casablanca",
    "morocco":         "Casablanca",
    "algeria":         "Algiers",
    "argelia":         "Algiers",
    "argélia":         "Algiers",
    "nigeria":         "Lagos (Apapa)",
    "nigeriana":       "Lagos (Apapa)",
    "kenya":           "Mombasa",
    "quenia":          "Mombasa",
    # América Latina
    "mexico":          "Veracruz",
    "méxico":          "Veracruz",
    "colombia":        "Cartagena",
    "colômbia":        "Cartagena",
    "peru":            "Callao",
    "chile":           "San Antonio",
    "cuba":            "Havana",
    # Europa
    "espanha":         "Barcelona",
    "spain":           "Barcelona",
    "portugal":        "Leixões",
    "italia":          "Genova",
    "itália":          "Genova",
    "grecia":          "Pireu",
    "grécia":          "Pireu",
    "greece":          "Pireu",
}


def _inferir_porto(pais: str) -> str:
    """Retorna o porto padrão para o país, ou string vazia se desconhecido."""
    if not pais:
        return ""
    key = pais.lower().strip().rstrip(".")
    # Tenta match exato, depois parcial
    if key in _PORTO_FALLBACK:
        return _PORTO_FALLBACK[key]
    for k, v in _PORTO_FALLBACK.items():
        if k in key or key in k:
            return v
    return ""


def _make_blank_record(fonte: str, text: str) -> dict:
    """Retorna um registro CRM em branco com metadados."""
    r = {k: "" for k in _CRM_FIELDS}
    r["fonte"]      = fonte
    r["created_at"] = datetime.datetime.now().isoformat()
    r["raw_input"]  = text
    r["error"]      = None
    return r


def _enrich_record(record: dict, fonte: str, text: str) -> dict:
    """Aplica enriquecimentos pós-extração: fallback de porto, fonte padrão."""
    if not record.get("fonte"):
        record["fonte"] = fonte
    if not record.get("raw_input"):
        record["raw_input"] = text
    if not record.get("created_at"):
        record["created_at"] = datetime.datetime.now().isoformat()

    # ── Inferência de porto por país ──────────────────────────────────────────
    # Mensagens WhatsApp frequentemente dizem só "CIF Turquia" sem porto.
    if not record.get("porto_destino") and record.get("pais_destino"):
        porto_inf = _inferir_porto(record["pais_destino"])
        if porto_inf:
            record["porto_destino"] = porto_inf
            logger.info("Porto inferido por país '%s' → %s",
                        record["pais_destino"], porto_inf)
    return record


def extract_crm(
    text: str,
    fonte: str = "Outro",
    model: str = "claude-haiku-4-5",
) -> dict:
    """
    Extrai campos CRM de texto livre via Claude API.
    Quando a mensagem contém múltiplas commodities, retorna apenas o PRIMEIRO
    registro (compatibilidade retroativa). Use extract_crm_multi() para obter
    todos os registros separados.

    Returns:
        dict com campos CRM + metadata (created_at, raw_input, error)
    """
    records = extract_crm_multi(text, fonte=fonte, model=model)
    return records[0] if records else _make_blank_record(fonte, text)


def extract_crm_multi(
    text: str,
    fonte: str = "Outro",
    model: str = "claude-haiku-4-5",
) -> list[dict]:
    """
    Extrai campos CRM de texto livre via Claude API.
    Retorna UMA LISTA de registros — um por commodity identificada na mensagem.
    Mensagens com Soja + Milho geram 2 registros independentes.

    Args:
        text:   Mensagem / texto de entrada
        fonte:  Canal de origem (WhatsApp, E-mail, LinkedIn, Outro)
        model:  Modelo Claude a usar

    Returns:
        Lista de dicts com campos CRM + metadata.
    """
    _campos_str = "\n".join(f'  "{k}": {v}' for k, v in _CRM_FIELDS.items())
    prompt = f"""Extraia os campos CRM da mensagem abaixo.

Se a mensagem contiver MÚLTIPLAS commodities distintas (ex: Soja E Milho),
retorne um ARRAY com um objeto JSON por commodity.
Se for uma só commodity, retorne um array com 1 objeto.

Cada objeto deve ter exatamente estas chaves:
{{
{_campos_str}
}}

Mensagem ({fonte}):
\"\"\"
{text}
\"\"\"

Responda APENAS com o array JSON, sem markdown, sem explicação."""

    fallback = _make_blank_record(fonte, text)

    try:
        import anthropic
        client = anthropic.Anthropic(timeout=40.0)
        msg = client.messages.create(
            model=model,
            max_tokens=1024,   # mais tokens para array com múltiplos registros
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()

        # Remove markdown code block se presente
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        parsed = json.loads(raw)

        # Normaliza: objeto simples → lista de 1
        if isinstance(parsed, dict):
            parsed = [parsed]

        records = []
        for item in parsed:
            r = _make_blank_record(fonte, text)
            for k in _CRM_FIELDS:
                r[k] = item.get(k, "")
            r = _enrich_record(r, fonte, text)
            records.append(r)
            logger.info(
                "CRM extraído: commodity=%s volume=%s porto=%s confianca=%s",
                r["commodity"], r["volume"], r["porto_destino"], r["confianca"]
            )

        return records if records else [fallback]

    except Exception as exc:
        logger.exception("extract_crm_multi falhou")
        fallback["error"] = str(exc)
        return [fallback]


def save_crm_record(record: dict, db_session=None) -> Optional[int]:
    """
    Salva registro CRM no banco de dados local.
    Retorna o ID do registro ou None se falhar.
    """
    try:
        from models.database import get_session
        import sqlalchemy as sa

        sess = db_session or get_session()
        # Tenta inserir na tabela crm_leads (criada por create_crm_table se existir)
        result = sess.execute(
            sa.text("""
                INSERT INTO crm_leads
                    (nome, empresa, commodity, volume, porto_destino, pais_destino,
                     incoterm, status_lead, observacoes, fonte, confianca,
                     raw_input, created_at)
                VALUES
                    (:nome, :empresa, :commodity, :volume, :porto_destino, :pais_destino,
                     :incoterm, :status_lead, :observacoes, :fonte, :confianca,
                     :raw_input, :created_at)
            """),
            {k: record.get(k, "") for k in [
                "nome", "empresa", "commodity", "volume", "porto_destino",
                "pais_destino", "incoterm", "status_lead", "observacoes",
                "fonte", "confianca", "raw_input", "created_at"
            ]}
        )
        sess.commit()
        return result.lastrowid
    except Exception as exc:
        logger.warning("save_crm_record falhou: %s", exc)
        return None


def create_crm_table(engine=None) -> bool:
    """Cria a tabela crm_leads se não existir."""
    try:
        from models.database import get_engine
        import sqlalchemy as sa
        eng = engine or get_engine()
        with eng.connect() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS crm_leads (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome          TEXT    DEFAULT '',
                    empresa       TEXT    DEFAULT '',
                    commodity     TEXT    DEFAULT '',
                    volume        TEXT    DEFAULT '',
                    porto_destino TEXT    DEFAULT '',
                    pais_destino  TEXT    DEFAULT '',
                    incoterm      TEXT    DEFAULT '',
                    status_lead   TEXT    DEFAULT 'Novo',
                    observacoes   TEXT    DEFAULT '',
                    fonte         TEXT    DEFAULT 'Outro',
                    confianca     TEXT    DEFAULT 'Médio',
                    raw_input     TEXT    DEFAULT '',
                    created_at    TEXT    DEFAULT '',
                    updated_at    TEXT    DEFAULT ''
                )
            """))
            conn.commit()
        return True
    except Exception as exc:
        logger.warning("create_crm_table falhou: %s", exc)
        return False


def load_crm_leads(limit: int = 100, db_session=None) -> list[dict]:
    """Carrega leads CRM do banco de dados."""
    try:
        from models.database import get_session
        import sqlalchemy as sa
        sess = db_session or get_session()
        rows = sess.execute(
            sa.text("""
                SELECT id, nome, empresa, commodity, volume, porto_destino,
                       pais_destino, incoterm, status_lead, observacoes,
                       fonte, confianca, created_at
                FROM crm_leads
                ORDER BY id DESC
                LIMIT :lim
            """),
            {"lim": limit}
        ).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.warning("load_crm_leads falhou: %s", exc)
        return []
