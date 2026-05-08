"""
agents/lead_pipeline.py
=======================
Pipeline central de processamento de leads CRM da Samba Export.

Usado por:
  - comercial_hub.py (entrada manual via UI)
  - extractor_agent.py / whatsapp_intelligence_router.py (captura automática)

Etapas executadas em sequência:
  1. check_duplicate()     — verifica se empresa+commodity já existe no banco
  2. save_to_db()          — persiste em crm_leads (SQLite)
  3. sync_to_sheets()      — append na planilha "todos andamento" (opcional/confirmado)
  4. identify_next_steps() — retorna lista de ações recomendadas (LOI / ICPO / follow-up)

Cada etapa retorna {ok, detail} para exibição no painel de controle.
"""
from __future__ import annotations

import datetime
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Próximos passos por heurística ────────────────────────────────────────────

_NEXT_STEP_LOI  = "loi"
_NEXT_STEP_ICPO = "icpo"
_NEXT_STEP_FUP  = "followup"

# Campos que, se ausentes, indicam necessidade de LOI/ICPO
_CAMPOS_CRITICOS = ["volume", "incoterm", "pais_destino"]


def check_duplicate(record: dict, db_session=None) -> dict:
    """
    Verifica se já existe um lead com mesma empresa + commodity no banco.

    Returns:
        {
          "is_duplicate": bool,
          "existing_id":  int | None,
          "detail":       str,
        }
    """
    empresa   = (record.get("empresa") or "").strip().lower()
    commodity = (record.get("commodity") or "").strip().lower()

    if not empresa and not commodity:
        return {"is_duplicate": False, "existing_id": None,
                "detail": "Sem empresa/commodity para verificar duplicata."}

    try:
        from models.database import get_session
        import sqlalchemy as sa
        sess = db_session or get_session()

        conds = []
        if empresa:
            conds.append(f"LOWER(empresa) = :emp")
        if commodity:
            conds.append(f"LOWER(commodity) LIKE :comm")

        where = " AND ".join(conds) if conds else "1=0"
        params: dict[str, Any] = {}
        if empresa:
            params["emp"]  = empresa
        if commodity:
            params["comm"] = f"%{commodity}%"

        row = sess.execute(
            sa.text(f"SELECT id, empresa, commodity, created_at FROM crm_leads WHERE {where} ORDER BY id DESC LIMIT 1"),
            params,
        ).fetchone()

        if row:
            d = dict(row._mapping)
            detail = (f"Lead similar encontrado: #{d['id']} — "
                      f"{d['empresa'] or '?'} / {d['commodity'] or '?'} "
                      f"({d.get('created_at','')[:10]})")
            return {"is_duplicate": True, "existing_id": d["id"], "detail": detail}

        return {"is_duplicate": False, "existing_id": None,
                "detail": "Nenhum lead similar encontrado — registro novo."}

    except Exception as exc:
        logger.warning("check_duplicate falhou: %s", exc)
        return {"is_duplicate": False, "existing_id": None,
                "detail": f"Verificação de duplicata indisponível: {exc}"}


def save_to_db(record: dict, db_session=None) -> dict:
    """
    Salva o lead no banco SQLite via crm_agent.save_crm_record().

    Returns:
        {"ok": bool, "record_id": int | None, "detail": str}
    """
    try:
        from agents.crm_agent import save_crm_record, create_crm_table
        create_crm_table()
        rid = save_crm_record(record, db_session=db_session)
        if rid:
            return {"ok": True,  "record_id": rid,  "detail": f"Lead #{rid} salvo no banco."}
        return {"ok": False, "record_id": None, "detail": "Falha ao salvar no banco (sem ID retornado)."}
    except Exception as exc:
        logger.exception("save_to_db falhou")
        return {"ok": False, "record_id": None, "detail": f"Erro ao salvar: {exc}"}


def sync_to_sheets(record: dict, record_id: int | None = None) -> dict:
    """
    Faz append do lead na planilha "todos andamento" via GoogleSheetsSync.
    Mapeia os campos do CRM record para o formato esperado pelo sheets sync.

    Returns:
        {"ok": bool, "detail": str, "sheet_url": str}
    """
    SHEET_URL = ("https://docs.google.com/spreadsheets/d/"
                 "1ToNZxYYi0dPQkQ0bRE8W3DWXJxkzEVS4vLQyrj2VP9U/edit")

    try:
        from services.google_sheets_sync import GoogleSheetsSync

        # Monta nome canônico do deal (coluna A = JOB)
        empresa   = (record.get("empresa") or "").strip()
        commodity = (record.get("commodity") or "").strip()
        job_name  = f"{empresa} — {commodity}".strip(" —")
        if record_id:
            job_name = f"CRM#{record_id} | {job_name}"

        # Volume para viz_rapida
        vol_raw = str(record.get("volume") or "?")
        inc_raw = str(record.get("incoterm") or "?")

        deal_data: dict[str, Any] = {
            "name":          job_name,
            "created_at":    datetime.datetime.now(),
            "direcao":       "BID",          # lead de compra (padrão)
            "source_group":  record.get("fonte", "WhatsApp"),
            "source_sender": record.get("nome", ""),
            "status":        record.get("status_lead", "Novo"),
            "produto":       commodity,
            "comprador":     empresa or record.get("nome", ""),
            "fornecedor":    "Samba Export",
            "volume":        vol_raw,
            "volume_unit":   "MT",
            "incoterm":      inc_raw,
            "price":         None,
            "currency":      "USD",
            "spec":          record.get("observacoes", "")[:500],
            "destination":   record.get("porto_destino") or record.get("pais_destino", ""),
        }

        sync = GoogleSheetsSync()
        ok   = sync.append_deal_to_sheet(deal_data)

        if ok:
            return {"ok": True,
                    "detail": f"Lead incluído na planilha de controle.",
                    "sheet_url": SHEET_URL}
        return {"ok": False,
                "detail": "Sheets sync retornou False — verifique credenciais Google.",
                "sheet_url": SHEET_URL}

    except Exception as exc:
        logger.exception("sync_to_sheets falhou")
        return {"ok": False,
                "detail": f"Erro ao sincronizar com Sheets: {exc}",
                "sheet_url": SHEET_URL}


def identify_next_steps(record: dict) -> list[dict]:
    """
    Analisa o lead e retorna lista de próximos passos recomendados.

    Cada item:
        {
          "type":    str,   # "loi" | "icpo" | "followup" | "whatsapp"
          "label":   str,   # texto do botão
          "reason":  str,   # explicação
          "priority": int,  # 1=alta, 2=média, 3=baixa
        }
    """
    steps = []
    commodity = (record.get("commodity") or "").lower()
    status    = record.get("status_lead", "Novo")
    volume    = record.get("volume") or ""
    nome      = record.get("nome") or ""
    empresa   = record.get("empresa") or ""

    # Volume muito alto → suspeito, pede LOI/ICPO com urgência
    vol_mt = 0.0
    try:
        import re
        nums = re.findall(r"[\d.,]+", str(volume))
        if nums:
            vol_mt = float(nums[0].replace(",", "."))
            # se o número está em unidades menores que 1000 e o campo diz "milhões"
            if "milh" in str(volume).lower():
                vol_mt *= 1_000_000
    except Exception:
        pass

    volume_alto = vol_mt > 500_000  # >500k MT é anormal — exige LOI obrigatória

    # ── Pedir LOI ─────────────────────────────────────────────────────────────
    loi_reason = ""
    if volume_alto:
        loi_reason = f"Volume de {volume} é muito alto — LOI obrigatória para prosseguir."
    elif status in ("Novo", "Qualificado"):
        loi_reason = "Lead novo — solicitar LOI para confirmar intenção formal de compra."
    elif not nome and not empresa:
        loi_reason = "Contato sem identificação — LOI ajuda a qualificar o comprador."

    if loi_reason:
        steps.append({
            "type":     _NEXT_STEP_LOI,
            "label":    "📄 Gerar / Solicitar LOI",
            "reason":   loi_reason,
            "priority": 1 if volume_alto else 2,
        })

    # ── Pedir ICPO ────────────────────────────────────────────────────────────
    if volume_alto or "açúcar" in commodity or "sugar" in commodity:
        steps.append({
            "type":     _NEXT_STEP_ICPO,
            "label":    "📋 Solicitar ICPO",
            "reason":   ("Açúcar e volumes altos normalmente exigem ICPO "
                         "antes da SCO/FCO.") if "açúcar" in commodity or "sugar" in commodity
                        else "Volume alto exige ICPO do comprador.",
            "priority": 1 if volume_alto else 2,
        })

    # ── Follow-up WhatsApp ────────────────────────────────────────────────────
    steps.append({
        "type":     _NEXT_STEP_FUP,
        "label":    "💬 Agendar Follow-up WhatsApp",
        "reason":   "Confirmar recebimento e interesse — resposta em até 24h.",
        "priority": 2,
    })

    # ── Cotação formal ────────────────────────────────────────────────────────
    if record.get("incoterm") and record.get("pais_destino"):
        steps.append({
            "type":     "cotacao",
            "label":    "📊 Gerar Price Indication",
            "reason":   (f"Incoterm ({record['incoterm']}) e destino "
                         f"({record.get('porto_destino') or record['pais_destino']}) "
                         f"definidos — pronto para gerar cotação formal."),
            "priority": 2,
        })

    # Ordena por prioridade
    steps.sort(key=lambda s: s["priority"])
    return steps


def run_pipeline(record: dict, sync_sheets: bool = False,
                 db_session=None) -> dict:
    """
    Executa o pipeline completo para um único lead.

    Args:
        record:      dict com campos CRM (saída de extract_crm_multi)
        sync_sheets: se True, faz append na planilha de controle
        db_session:  sessão SQLAlchemy opcional (para testes)

    Returns:
        {
          "duplicate":   {"is_duplicate", "existing_id", "detail"},
          "db":          {"ok", "record_id", "detail"},
          "sheets":      {"ok", "detail", "sheet_url"} | None,
          "next_steps":  list[dict],
          "record_id":   int | None,
        }
    """
    result: dict[str, Any] = {
        "duplicate":  {},
        "db":         {},
        "sheets":     None,
        "next_steps": [],
        "record_id":  None,
    }

    # 1. Deduplicação
    result["duplicate"] = check_duplicate(record, db_session=db_session)

    # 2. Salva no banco (mesmo se duplicata — a decisão é do operador)
    result["db"] = save_to_db(record, db_session=db_session)
    result["record_id"] = result["db"].get("record_id")

    # 3. Sheets (somente se solicitado pelo operador)
    if sync_sheets:
        result["sheets"] = sync_to_sheets(record, record_id=result["record_id"])

    # 4. Próximos passos
    result["next_steps"] = identify_next_steps(record)

    return result
