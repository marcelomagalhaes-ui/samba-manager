"""
dashboards/quick_quote_chat.py
==============================
Quick Quote — Janela de Conversa Comercial.

O trader digita em linguagem natural ("Soja CIF Vietnã 25000 MT") e o sistema:
  1. Usa Gemini para extrair produto, incoterm, destino e volume
  2. Calcula EXW / FOB / CIF ao vivo com o motor de pricing
  3. Retorna resposta direta + perguntas comerciais de qualificação
  4. Oferece ações: preencher form completo, criar deal, gerar mensagem WhatsApp
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ─── imports do motor de pricing (reusa tudo que já existe) ──────────────────
from dashboards.pricing_widget import (
    _DESTINOS_CIF,
    _DEST_KEYS,
    _NAVIOS,
    _PORTOS,
    _PRODUTOS,
    _calcular_frete_maritimo,
    _calcular_stack,
    _get_snapshot,
    _navio_para_porto_volume,
)

# ─────────────────────────────────────────────────────────────────────────────
# NORMALIZAÇÃO — remove acentos, lowercase, espaços extras
# Garante que "Vietnã", "Vietnam", "Viet Nam", "VIETNAM" → "vietnam"
# ─────────────────────────────────────────────────────────────────────────────

import unicodedata
import re as _re

def _norm(s: str) -> str:
    """Normaliza string: sem acento, lowercase, sem pontuação extra, sem espaços duplos."""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")  # remove diacríticos
    s = s.lower().strip()
    s = _re.sub(r"[^\w\s]", " ", s)   # pontuação → espaço
    s = _re.sub(r"\s+", " ", s).strip()
    return s


# ─────────────────────────────────────────────────────────────────────────────
# MAPEAMENTOS DE LINGUAGEM NATURAL → CHAVES INTERNAS
# Todas as chaves já ficam normalizadas para comparação rápida.
# ─────────────────────────────────────────────────────────────────────────────

# Produto: qualquer variação → chave interna
_PRODUTO_ALIAS_RAW: list[tuple[str, str]] = [
    # soja
    ("soja",              "SOY"),
    ("soy",               "SOY"),
    ("soybeans",          "SOY"),
    ("soybean",           "SOY"),
    ("feijao de soja",    "SOY"),
    ("yellow soybeans",   "SOY"),
    # milho
    ("milho",             "CORN"),
    ("corn",              "CORN"),
    ("yellow corn",       "CORN"),
    ("maiz",              "CORN"),
    # açúcar VHP (padrão)
    ("acucar",            "SUGAR_VHP"),
    ("sugar",             "SUGAR_VHP"),
    ("vhp",               "SUGAR_VHP"),
    ("sugar vhp",         "SUGAR_VHP"),
    ("acucar vhp",        "SUGAR_VHP"),
    ("acucar bruto",      "SUGAR_VHP"),
    ("raw sugar",         "SUGAR_VHP"),
    # açúcar IC45
    ("ic45",              "SUGAR_IC45"),
    ("icumsa 45",         "SUGAR_IC45"),
    ("icumsa45",          "SUGAR_IC45"),
    ("acucar ic45",       "SUGAR_IC45"),
    ("acucar refinado",   "SUGAR_IC45"),
    ("white sugar",       "SUGAR_IC45"),
    # açúcar IC150
    ("ic150",             "SUGAR_IC150"),
    ("icumsa 150",        "SUGAR_IC150"),
    ("icumsa150",         "SUGAR_IC150"),
    ("acucar ic150",      "SUGAR_IC150"),
    ("cristal",           "SUGAR_IC150"),
    ("crystal sugar",     "SUGAR_IC150"),
]
# normaliza as chaves uma vez
_PRODUTO_ALIAS: list[tuple[str, str]] = [(_norm(k), v) for k, v in _PRODUTO_ALIAS_RAW]


# Destino: qualquer variação → label exato do _DEST_KEYS em pricing_widget.py
# 28 destinos com distinção Norte/Sul China, Índia Oeste/Leste, Jeddah vs Dubai, etc.
_DESTINO_ALIAS_RAW: list[tuple[str, str]] = [
    # ── CHINA ─────────────────────────────────────────────────────
    ("china norte",         "China — Shanghai / Norte"),
    ("north china",         "China — Shanghai / Norte"),
    ("xangai",              "China — Shanghai / Norte"),
    ("shanghai",            "China — Shanghai / Norte"),
    ("qingdao",             "China — Shanghai / Norte"),
    ("tianjin",             "China — Shanghai / Norte"),
    ("dalian",              "China — Shanghai / Norte"),
    ("china sul",           "China — Guangzhou / Sul"),
    ("south china",         "China — Guangzhou / Sul"),
    ("guangzhou",           "China — Guangzhou / Sul"),
    ("shenzhen",            "China — Guangzhou / Sul"),
    ("ningbo",              "China — Guangzhou / Sul"),
    ("zhoushan",            "China — Guangzhou / Sul"),
    ("china",               "China — Shanghai / Norte"),
    # ── VIETNA ────────────────────────────────────────────────────
    ("vietnam",             "Vietnã — Ho Chi Minh"),
    ("vietna",              "Vietnã — Ho Chi Minh"),
    ("viet nam",            "Vietnã — Ho Chi Minh"),
    ("viet-nam",            "Vietnã — Ho Chi Minh"),
    ("vietname",            "Vietnã — Ho Chi Minh"),
    ("ho chi minh",         "Vietnã — Ho Chi Minh"),
    ("haiphong",            "Vietnã — Ho Chi Minh"),
    ("hanoi",               "Vietnã — Ho Chi Minh"),
    ("cai mep",             "Vietnã — Ho Chi Minh"),
    # ── INDONESIA ─────────────────────────────────────────────────
    ("indonesia",           "Indonésia — Jakarta"),
    ("indonecia",           "Indonésia — Jakarta"),
    ("jakarta",             "Indonésia — Jakarta"),
    ("surabaya",            "Indonésia — Jakarta"),
    # ── INDIA ─────────────────────────────────────────────────────
    ("india oeste",         "Índia — Mundra / Kandla"),
    ("india west",          "Índia — Mundra / Kandla"),
    ("mundra",              "Índia — Mundra / Kandla"),
    ("kandla",              "Índia — Mundra / Kandla"),
    ("mumbai",              "Índia — Mundra / Kandla"),
    ("nhava sheva",         "Índia — Mundra / Kandla"),
    ("india leste",         "Índia — Chennai / Leste"),
    ("india east",          "Índia — Chennai / Leste"),
    ("chennai",             "Índia — Chennai / Leste"),
    ("madras",              "Índia — Chennai / Leste"),
    ("kolkata",             "Índia — Chennai / Leste"),
    ("india",               "Índia — Mundra / Kandla"),
    # ── COREIA / JAPAO ────────────────────────────────────────────
    ("coreia",              "Coreia do Sul — Busan"),
    ("korea",               "Coreia do Sul — Busan"),
    ("south korea",         "Coreia do Sul — Busan"),
    ("busan",               "Coreia do Sul — Busan"),
    ("japao",               "Japão — Yokohama"),
    ("japan",               "Japão — Yokohama"),
    ("yokohama",            "Japão — Yokohama"),
    ("nagoya",              "Japão — Yokohama"),
    ("kobe",                "Japão — Yokohama"),
    ("osaka",               "Japão — Yokohama"),
    # ── SUDESTE ASIATICO ──────────────────────────────────────────
    ("malasia",             "Malásia — Port Klang"),
    ("malaysia",            "Malásia — Port Klang"),
    ("port klang",          "Malásia — Port Klang"),
    ("tailandia",           "Tailândia — Laem Chabang"),
    ("thailand",            "Tailândia — Laem Chabang"),
    ("laem chabang",        "Tailândia — Laem Chabang"),
    ("bangkok",             "Tailândia — Laem Chabang"),
    ("singapura",           "Singapura"),
    ("singapore",           "Singapura"),
    ("sri lanka",           "Sri Lanka — Colombo"),
    ("colombo",             "Sri Lanka — Colombo"),
    # ── ORIENTE MEDIO ─────────────────────────────────────────────
    ("jeddah",              "Arábia Saudita — Jeddah"),
    ("jida",                "Arábia Saudita — Jeddah"),
    ("arabia saudita",      "Arábia Saudita — Jeddah"),
    ("saudi",               "Arábia Saudita — Jeddah"),
    ("arabia",              "Arábia Saudita — Jeddah"),
    ("mar vermelho",        "Arábia Saudita — Jeddah"),
    ("red sea",             "Arábia Saudita — Jeddah"),
    ("dubai",               "Golfo Pérsico — Dubai / EAU"),
    ("eau",                 "Golfo Pérsico — Dubai / EAU"),
    ("uae",                 "Golfo Pérsico — Dubai / EAU"),
    ("abu dhabi",           "Golfo Pérsico — Dubai / EAU"),
    ("golfo persico",       "Golfo Pérsico — Dubai / EAU"),
    ("golfo arabico",       "Golfo Pérsico — Dubai / EAU"),
    ("gulf",                "Golfo Pérsico — Dubai / EAU"),
    ("oman",                "Golfo Pérsico — Dubai / EAU"),
    ("qatar",               "Golfo Pérsico — Dubai / EAU"),
    ("dammam",              "Golfo Pérsico — Dubai / EAU"),
    ("kuwait",              "Golfo Pérsico — Dubai / EAU"),
    ("oriente medio",       "Golfo Pérsico — Dubai / EAU"),
    ("middle east",         "Golfo Pérsico — Dubai / EAU"),
    # ── AFRICA ────────────────────────────────────────────────────
    ("egito",               "Egito — Alexandria"),
    ("egypt",               "Egito — Alexandria"),
    ("alexandria",          "Egito — Alexandria"),
    ("port said",           "Egito — Alexandria"),
    ("marrocos",            "Marrocos — Tânger"),
    ("morocco",             "Marrocos — Tânger"),
    ("tanger",              "Marrocos — Tânger"),
    ("tanger med",          "Marrocos — Tânger"),
    ("africa norte",        "Marrocos — Tânger"),
    ("north africa",        "Marrocos — Tânger"),
    ("nigeria",             "Nigéria — Lagos"),
    ("lagos",               "Nigéria — Lagos"),
    ("gana",                "Gana — Tema"),
    ("ghana",               "Gana — Tema"),
    ("tema",                "Gana — Tema"),
    ("africa do sul",       "África do Sul — Durban"),
    ("south africa",        "África do Sul — Durban"),
    ("durban",              "África do Sul — Durban"),
    ("quenia",              "África do Sul — Durban"),
    ("kenya",               "África do Sul — Durban"),
    ("tanzania",            "África do Sul — Durban"),
    ("angola",              "Nigéria — Lagos"),
    ("mocambique",          "Nigéria — Lagos"),
    ("senegal",             "Nigéria — Lagos"),
    ("africa subsaariana",  "Nigéria — Lagos"),
    ("west africa",         "Nigéria — Lagos"),
    ("africa ocidental",    "Nigéria — Lagos"),
    ("africa",              "Nigéria — Lagos"),
    # ── EUROPA ────────────────────────────────────────────────────
    ("rotterdam",           "Europa NW — Rotterdam"),
    ("amsterdam",           "Europa NW — Rotterdam"),
    ("hamburgo",            "Europa NW — Rotterdam"),
    ("hamburg",             "Europa NW — Rotterdam"),
    ("antuérpia",           "Europa NW — Rotterdam"),
    ("antwerp",             "Europa NW — Rotterdam"),
    ("norte europa",        "Europa NW — Rotterdam"),
    ("north europe",        "Europa NW — Rotterdam"),
    ("europa nw",           "Europa NW — Rotterdam"),
    ("northwestern europe", "Europa NW — Rotterdam"),
    ("le havre",            "Europa NW — Rotterdam"),
    ("europa",              "Europa NW — Rotterdam"),
    ("europe",              "Europa NW — Rotterdam"),
    ("valencia",            "Mediterrâneo — Valencia"),
    ("barcelona",           "Mediterrâneo — Valencia"),
    ("tarragona",           "Mediterrâneo — Valencia"),
    ("espanha",             "Mediterrâneo — Valencia"),
    ("spain",               "Mediterrâneo — Valencia"),
    ("italia",              "Mediterrâneo — Valencia"),
    ("italy",               "Mediterrâneo — Valencia"),
    ("genova",              "Mediterrâneo — Valencia"),
    ("mediterraneo",        "Mediterrâneo — Valencia"),
    ("piraeus",             "Grécia — Pireu"),
    ("pireu",               "Grécia — Pireu"),
    ("grecia",              "Grécia — Pireu"),
    ("greece",              "Grécia — Pireu"),
    ("atenas",              "Grécia — Pireu"),
    # ── AMERICAS ──────────────────────────────────────────────────
    ("new orleans",         "EUA — Golfo (New Orleans)"),
    ("houston",             "EUA — Golfo (New Orleans)"),
    ("gulf of mexico",      "EUA — Golfo (New Orleans)"),
    ("texas",               "EUA — Golfo (New Orleans)"),
    ("louisiana",           "EUA — Golfo (New Orleans)"),
    ("norfolk",             "EUA — Costa Leste (Norfolk)"),
    ("baltimore",           "EUA — Costa Leste (Norfolk)"),
    ("new york",            "EUA — Costa Leste (Norfolk)"),
    ("east coast",          "EUA — Costa Leste (Norfolk)"),
    ("costa leste",         "EUA — Costa Leste (Norfolk)"),
    ("eua",                 "EUA — Golfo (New Orleans)"),
    ("usa",                 "EUA — Golfo (New Orleans)"),
    ("estados unidos",      "EUA — Golfo (New Orleans)"),
    ("united states",       "EUA — Golfo (New Orleans)"),
    ("canada",              "Canadá — Vancouver"),
    ("vancouver",           "Canadá — Vancouver"),
    ("mexico",              "México — Veracruz"),
    ("veracruz",            "México — Veracruz"),
    ("argentina",           "Argentina — Rosário"),
    ("rosario",             "Argentina — Rosário"),
    ("buenos aires",        "Argentina — Rosário"),
    ("chile",               "Chile — San Antonio"),
    ("san antonio",         "Chile — San Antonio"),
    ("valparaiso",          "Chile — San Antonio"),
]
# normaliza as chaves uma vez
_DESTINO_ALIAS: list[tuple[str, str]] = [(_norm(k), v) for k, v in _DESTINO_ALIAS_RAW]


def _match_destino(texto_norm: str) -> str | None:
    """
    Busca destino no texto normalizado.
    Estratégia: primeiro match exato, depois match de substring (maior primeiro).
    """
    # 1. match exato
    for k, v in _DESTINO_ALIAS:
        if k == texto_norm:
            return v
    # 2. substring — ordena por tamanho decrescente (evita "india" casar antes de "arabia saudita")
    for k, v in sorted(_DESTINO_ALIAS, key=lambda x: -len(x[0])):
        if k in texto_norm:
            return v
    return None


def _match_produto(texto_norm: str) -> str | None:
    """Busca produto no texto normalizado — match exato primeiro, depois substring."""
    for k, v in _PRODUTO_ALIAS:
        if k == texto_norm:
            return v
    for k, v in sorted(_PRODUTO_ALIAS, key=lambda x: -len(x[0])):
        if k in texto_norm:
            return v
    return None

_PORTO_DEFAULT: dict[str, str] = {
    # produto → porto preferencial por rota/logística
    "SOY":        "Outeiro (Belém/PA)",
    "CORN":       "Outeiro (Belém/PA)",
    "SUGAR_VHP":  "Santos (SP)",
    "SUGAR_IC45": "Santos (SP)",
    "SUGAR_IC150":"Santos (SP)",
}


# ─────────────────────────────────────────────────────────────────────────────
# PARSER GEMINI — extrai estrutura de linguagem natural
# ─────────────────────────────────────────────────────────────────────────────

def _parse_query_gemini(texto: str) -> dict:
    """
    Estratégia de parsing em duas camadas:
      1. Parser local — zero latência, 60+ aliases robustos (sempre roda primeiro)
      2. Gemini — só chamado se local NÃO identificou produto OU destino,
                  com timeout de 4 s para não travar a UI.
    """
    # ── Camada 1: local instantâneo ──────────────────────────────────────────
    local = _parse_query_local(texto)

    # Se o parser local já identificou produto E destino (ou é FOB/EXW sem destino)
    _inc_no_dest = local.get("incoterm") in ("EXW", "FAS", "FOB")
    if local.get("produto") and (local.get("destino") or _inc_no_dest):
        return local   # retorna imediatamente, sem chamar Gemini

    # ── Camada 2: Gemini com timeout de 4 s (só se local falhou em algo) ─────
    try:
        import concurrent.futures
        import google.generativeai as genai

        api_key = os.getenv("GEMINI_API_KEY") or ""
        try:
            api_key = api_key or st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass
        if not api_key:
            return local  # sem chave → devolve o que o parser local conseguiu

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")
        prompt = f"""
Voce e um assistente de trading de commodities agricolas para exportacao do Brasil.
Analise a mensagem abaixo e extraia as informacoes em JSON puro (sem markdown, sem explicacao).

IMPORTANTE — grafias incorretas ou variantes sao comuns. Normalize sempre:
- "Vietnam", "Vietna", "Viet Nam", "Vietname" → destino = "Vietnam"
- "Indonesia", "Indonecia" → destino = "Indonesia"
- "India", "Indie" → destino = "India"
- "Egito", "Egypt" → destino = "Egito"
- "China", "Xina", "PRC" → destino = "China"
- "oriente medio", "middle east", "golfo" → destino = "Oriente Medio"
- "Europa", "Rotterdam", "Hamburg" → destino = "Europa NW"
- "EUA", "USA", "Estados Unidos" → destino = "EUA"

Campos:
- "produto": um de [SOY, CORN, SUGAR_VHP, SUGAR_IC45, SUGAR_IC150] ou null
- "incoterm": um de [EXW, FAS, FOB, CFR, CIF] ou null (padrao CIF se nao informado)
- "destino": nome canonico do destino ou null
- "volume_mt": inteiro em toneladas metricas (padrao 25000)
- "porto_br": porto brasileiro de embarque ou null

Mensagem: "{texto}"

Responda SOMENTE com o JSON.
Exemplo: {{"produto":"SOY","incoterm":"CIF","destino":"Vietnam","volume_mt":25000,"porto_br":null}}
"""
        def _call_gemini():
            resp = model.generate_content(prompt)
            raw = resp.text.strip().strip("```json").strip("```").strip()
            return json.loads(raw)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_call_gemini)
            try:
                parsed = fut.result(timeout=4)  # máximo 4 segundos
            except concurrent.futures.TimeoutError:
                return local  # Gemini demorou → usa o que o local conseguiu

        # Mescla: Gemini preenche apenas o que o local não conseguiu
        return {
            "produto":   local.get("produto") or parsed.get("produto"),
            "incoterm":  local.get("incoterm") or parsed.get("incoterm") or "CIF",
            "destino":   local.get("destino") or parsed.get("destino"),
            "volume_mt": local.get("volume_mt") or int(parsed.get("volume_mt") or 25_000),
            "porto_br":  local.get("porto_br") or parsed.get("porto_br"),
        }
    except Exception:
        return local


def _parse_query_local(texto: str) -> dict:
    """Parser de fallback sem IA — normaliza e usa aliases robustos."""
    t = _norm(texto)

    produto  = _match_produto(t)
    destino  = _match_destino(t)

    incoterm = None
    for inc in ["aswp", "cif", "cfr", "fob", "fas", "exw"]:
        if inc in t.split():   # word-boundary: "cif" mas não "pacific"
            incoterm = inc.upper()
            break
    # segunda passagem sem word boundary (captura "CIF" colado a outro texto)
    if not incoterm:
        for inc in ["cif", "cfr", "fob", "fas", "exw"]:
            if inc in t:
                incoterm = inc.upper()
                break
    if not incoterm:
        incoterm = "CIF"

    # volume: "25000", "25.000", "25,000", "25k", "25 mil"
    vol = 25_000
    m = _re.search(r"(\d[\d.,]*)\s*(mil\b|k\b|mt\b|ton\b|t\b)?", t)
    if m:
        raw_n = m.group(1).replace(".", "").replace(",", ".")
        try:
            n = float(raw_n)
            sufixo = (m.group(2) or "").strip()
            if sufixo in ("mil", "k"):
                n *= 1_000
            if 100 <= n <= 500_000:
                vol = int(n)
        except ValueError:
            pass

    return {
        "produto":   produto,
        "incoterm":  incoterm,
        "destino":   destino,
        "volume_mt": vol,
        "porto_br":  None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MOTOR DE RESPOSTA
# ─────────────────────────────────────────────────────────────────────────────

def _calcular_quick_quote(parsed: dict, snap: dict) -> dict | None:
    """
    Executa o cálculo de pricing a partir do parsed e retorna
    um dict com todos os valores prontos para exibição.
    Retorna None se dados insuficientes.
    """
    produto_key = parsed.get("produto")
    if not produto_key or produto_key not in _PRODUTOS:
        return None

    incoterm  = (parsed.get("incoterm") or "CIF").upper()
    destino   = parsed.get("destino")
    volume_mt = int(parsed.get("volume_mt") or 25_000)

    produto_meta = _PRODUTOS[produto_key]

    # Porto de embarque
    porto_label = _PORTO_DEFAULT.get(produto_key, "Santos (SP)")
    # Tenta casar nome do porto_br informado pelo usuário
    if parsed.get("porto_br"):
        pb = parsed["porto_br"].lower()
        for k in _PORTOS:
            if pb in k.lower():
                porto_label = k
                break
    porto_meta = _PORTOS[porto_label]
    porto_code = porto_meta["code"]

    # Preço FOB
    stack = _calcular_stack(snap, porto_meta, produto_meta, volume_mt, None)

    result = {
        "produto_key":  produto_key,
        "produto_label": produto_meta["label"],
        "incoterm":     incoterm,
        "destino":      destino,
        "volume_mt":    volume_mt,
        "porto_label":  porto_label,
        "regiao":       porto_meta["regiao"],
        "exw_usd":      stack["exw_usd"],
        "fob_usd":      stack["fob_usd"],
        "fob_brl":      stack["fob_brl"],
        "fob_saca":     stack["fob_saca"],
        "basis":        stack["basis"],
        "fx":           stack["fx"],
        "bolsa":        stack["bolsa"],
        "kg_saca":      stack["kg_saca"],
        "fonte":        stack["fonte"],
        "cfr_usd":      None,
        "cif_usd":      None,
        "frete_mt":     None,
        "seguro_mt":    None,
        "dist_nm":      None,
        "transit_days": None,
        "navio_key":    None,
    }

    # Frete marítimo (para CFR / CIF)
    if incoterm in ("CFR", "CIF") and destino:
        # normaliza o destino (captura qualquer grafia)
        dest_key = _match_destino(_norm(destino)) or destino
        if dest_key in _DEST_KEYS or dest_key in list(_DEST_KEYS.values()):
            # normaliza para label correto
            dest_label = dest_key if dest_key in _DEST_KEYS else next(
                (l for l, v in _DEST_KEYS.items() if v == dest_key), None
            )
            if dest_label:
                bunker   = snap.get("bunker_vlsfo", 550.0) or 550.0
                hire_mkt = snap.get("daily_hire", 0) or 0
                navio_key = _navio_para_porto_volume(volume_mt, porto_code)
                fr = _calcular_frete_maritimo(
                    porto_code, dest_label, volume_mt, navio_key, bunker, hire_mkt
                )

                # ── Granel: usa apenas custo de ida (sem lastro de retorno).
                # ── Container: mantém freight_per_mt (ida + lastro — preço fechado
                #    de mercado já embute retorno). Detectado pelo nome do navio.
                is_container = "container" in (navio_key or "").lower() or \
                               "feeder" in (navio_key or "").lower()
                frete_mt = fr["freight_per_mt"] if is_container \
                           else fr["freight_min_per_mt"]

                # Seguro: 0,3% × 110% × CFR
                cfr = stack["fob_usd"] + frete_mt
                seg = round(cfr * 1.10 * 0.003, 2)
                cif = round(cfr + seg, 2)
                result.update({
                    "cfr_usd":      round(cfr, 2),
                    "cif_usd":      cif,
                    "frete_mt":     frete_mt,
                    "seguro_mt":    seg,
                    "dist_nm":      fr["dist_nm"],
                    "transit_days": round(fr["sea_days"], 0),
                    "navio_key":    navio_key,
                    "is_container": is_container,
                })

    return result


# ─────────────────────────────────────────────────────────────────────────────
# RENDER DA RESPOSTA
# ─────────────────────────────────────────────────────────────────────────────

def _render_quote_response(r: dict):
    """Renderiza o card de resposta da cotação no chat."""
    prod    = r["produto_label"].upper()
    inc     = r["incoterm"]
    dest    = r["destino"] or "—"
    vol     = r["volume_mt"]
    porto   = r["porto_label"].split(" (")[0].upper()
    regiao  = r["regiao"]
    fx      = r["fx"]
    kg      = r["kg_saca"]

    # linha principal de preço
    if inc == "CIF" and r["cif_usd"]:
        preco_destaque = r["cif_usd"]
        label_destaque = f"CIF {dest.upper()}"
        cor_destaque   = "#FA8200"
    elif inc == "CFR" and r["cfr_usd"]:
        preco_destaque = r["cfr_usd"]
        label_destaque = f"CFR {dest.upper()}"
        cor_destaque   = "#FA8200"
    else:
        preco_destaque = r["fob_usd"]
        label_destaque = f"FOB {porto}"
        cor_destaque   = "#FA8200"

    preco_brl  = round(preco_destaque * fx * kg / 1000, 2)
    total_usd  = round(preco_destaque * vol / 1_000_000, 2)

    # stack de linhas
    linhas = [
        ("EXW Origem",  f"$ {r['exw_usd']:,.2f} / MT"),
        ("FOB " + porto, f"$ {r['fob_usd']:,.2f} / MT"),
    ]
    if r["frete_mt"]:
        # Rótulo diferencia granel (somente ida) de container (com lastro embutido)
        _is_ctn = r.get("is_container", False)
        _fr_lbl = "Frete marítimo (container)" if _is_ctn else "Frete marítimo (viagem ida)"
        linhas.append((_fr_lbl, f"$ {r['frete_mt']:,.2f} / MT"))
    if r["seguro_mt"]:
        linhas.append(("Seguro (0,3% × 110% CFR)", f"$ {r['seguro_mt']:,.2f} / MT"))
    if r["cfr_usd"]:
        linhas.append(("CFR " + dest, f"$ {r['cfr_usd']:,.2f} / MT"))
    if r["cif_usd"] and inc == "CIF":
        linhas.append(("CIF " + dest, f"$ {r['cif_usd']:,.2f} / MT"))

    linhas_html = "".join(
        f'<div style="display:flex;justify-content:space-between;padding:5px 0;'
        f'border-bottom:1px solid #F0EBE3;font-family:Montserrat,sans-serif">'
        f'<span style="font-size:11px;color:#6B7280">{l}</span>'
        f'<span style="font-size:11px;font-weight:700;color:#1A1A1A;font-family:monospace">{v}</span>'
        f'</div>'
        for l, v in linhas
    )

    # info extra (rota)
    rota_html = ""
    if r["dist_nm"]:
        navio_short  = (r["navio_key"] or "").split("(")[0].strip()
        _is_ctn      = r.get("is_container", False)
        _base_label  = "container" if _is_ctn else "granel · somente ida"
        rota_html = (
            f'<div style="font-size:10px;color:#9CA3AF;margin-top:8px;font-family:Montserrat,sans-serif">'
            f'{porto} → {dest} &nbsp;·&nbsp; {r["dist_nm"]:,} NM &nbsp;·&nbsp; '
            f'{int(r["transit_days"])} dias &nbsp;·&nbsp; {navio_short} &nbsp;·&nbsp; {_base_label}'
            f'</div>'
        )

    st.markdown(f"""
<div style="background:#FFFDF8;border:1px solid #F0E6D0;border-left:4px solid #FA8200;
border-radius:10px;padding:18px 20px;margin:8px 0;font-family:Montserrat,sans-serif">
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:14px;flex-wrap:wrap">
    <span style="font-size:9px;font-weight:700;letter-spacing:2px;color:#FA8200">{prod}</span>
    <span style="font-size:9px;color:#D1D5DB">·</span>
    <span style="font-size:9px;font-weight:700;letter-spacing:1.5px;color:#6B7280">{label_destaque}</span>
    <span style="font-size:9px;color:#D1D5DB">·</span>
    <span style="font-size:9px;color:#6B7280">{vol:,.0f} MT</span>
    <span style="margin-left:auto;font-size:9px;color:#9CA3AF">{r["fonte"]}</span>
  </div>
  <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:14px">
    <div>
      <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF;margin-bottom:2px">{label_destaque}</div>
      <div style="font-size:38px;font-weight:900;color:{cor_destaque};line-height:1">$ {preco_destaque:,.2f}</div>
      <div style="font-size:12px;color:#6B7280;margin-top:2px">USD / MT</div>
    </div>
    <div style="border-left:1px solid #E5E7EB;padding-left:16px">
      <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF;margin-bottom:2px">R$ / SACA ({kg}kg)</div>
      <div style="font-size:24px;font-weight:900;color:#1A1A1A;line-height:1">R$ {preco_brl:,.2f}</div>
      <div style="font-size:12px;color:#6B7280;margin-top:2px">câmbio {fx:.4f}</div>
    </div>
    <div style="border-left:1px solid #E5E7EB;padding-left:16px">
      <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF;margin-bottom:2px">TOTAL</div>
      <div style="font-size:20px;font-weight:900;color:#1A1A1A;line-height:1">$ {total_usd:.2f}M</div>
      <div style="font-size:12px;color:#6B7280;margin-top:2px">USD ({vol:,.0f} MT)</div>
    </div>
  </div>
  <div style="background:#F9F6F0;border-radius:8px;padding:10px 12px;margin-bottom:10px">
    {linhas_html}
  </div>
  {rota_html}
</div>
""", unsafe_allow_html=True)


def _render_followup_questions(r: dict):
    """Perguntas de qualificação comercial após a cotação."""
    st.markdown("""
<div style="background:#F0F9FF;border:1px solid #BAE6FD;border-radius:8px;
padding:12px 16px;margin:6px 0;font-family:Montserrat,sans-serif">
  <div style="font-size:9px;font-weight:700;letter-spacing:1.5px;color:#0369A1;margin-bottom:8px">
    QUALIFICACAO COMERCIAL
  </div>
  <div style="font-size:11px;color:#374151;line-height:2">
    Temos oferta real para este volume e prazo?<br>
    Comprador identificado ou consulta de mercado?<br>
    Qual o prazo de embarque desejado?<br>
    Ja ha LOI / ICPO ou e apenas indicativo?
  </div>
</div>
""", unsafe_allow_html=True)


def _gerar_mensagem_comercial(r: dict) -> str:
    """Gera mensagem comercial pronta para WhatsApp/email."""
    prod    = r["produto_label"]
    inc     = r["incoterm"]
    dest    = r["destino"] or "ASWP"
    vol     = r["volume_mt"]
    porto   = r["porto_label"].split(" (")[0]
    hoje    = datetime.now().strftime("%d/%m/%Y")

    if inc == "CIF" and r["cif_usd"]:
        preco = r["cif_usd"]
        inc_txt = f"CIF {dest}"
    elif inc == "CFR" and r["cfr_usd"]:
        preco = r["cfr_usd"]
        inc_txt = f"CFR {dest}"
    else:
        preco = r["fob_usd"]
        inc_txt = f"FOB {porto}"

    msg = (
        f"Indicativo de preco - {prod}\n"
        f"Incoterm: {inc_txt}\n"
        f"Volume: {vol:,} MT\n"
        f"Porto de origem: {porto}, Brasil\n"
        f"Preco: USD {preco:,.2f} / MT\n"
        f"Cambio referencia: R$ {r['fx']:.4f}/USD\n"
        f"Data de referencia: {hoje}\n\n"
        f"Preco indicativo sujeito a confirmacao de disponibilidade, "
        f"frete, inspecao SGS/CCIC e documentos finais. "
        f"Validade: 24 horas."
    )
    return msg


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT — renderiza o painel de chat
# ─────────────────────────────────────────────────────────────────────────────

def render_quick_quote():
    """Painel de cotacao rapida por linguagem natural."""

    # init session state
    if "qq_history" not in st.session_state:
        st.session_state.qq_history = []   # lista de dicts {role, content, result}
    if "qq_pending_result" not in st.session_state:
        st.session_state.qq_pending_result = None
    if "qq_msg_copiada" not in st.session_state:
        st.session_state.qq_msg_copiada = False

    snap = _get_snapshot()

    # header
    st.markdown("""
<div style="font-family:Montserrat,sans-serif;margin-bottom:14px">
  <div style="font-size:9px;font-weight:700;letter-spacing:2px;color:#FA8200;margin-bottom:3px">
    QUICK QUOTE
  </div>
  <div style="font-size:16px;font-weight:800;color:#1A1A1A">
    Cotacao Rapida por Conversa
  </div>
  <div style="font-size:11px;color:#6B7280;margin-top:3px">
    Digite o que precisa: produto, incoterm e destino. O sistema calcula na hora.
  </div>
</div>
""", unsafe_allow_html=True)

    # exemplos de consulta
    st.markdown("""
<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px">
  <span style="background:#1A1C24;color:#D1D5DB;font-size:10px;font-weight:600;
    padding:5px 12px;border-radius:20px;font-family:Montserrat,sans-serif;cursor:default">
    Soja CIF China 25000 MT
  </span>
  <span style="background:#1A1C24;color:#D1D5DB;font-size:10px;font-weight:600;
    padding:5px 12px;border-radius:20px;font-family:Montserrat,sans-serif;cursor:default">
    Acucar IC45 FOB Santos
  </span>
  <span style="background:#1A1C24;color:#D1D5DB;font-size:10px;font-weight:600;
    padding:5px 12px;border-radius:20px;font-family:Montserrat,sans-serif;cursor:default">
    Milho CIF Oriente Medio 50000
  </span>
  <span style="background:#1A1C24;color:#D1D5DB;font-size:10px;font-weight:600;
    padding:5px 12px;border-radius:20px;font-family:Montserrat,sans-serif;cursor:default">
    Soja FOB Outeiro
  </span>
</div>
""", unsafe_allow_html=True)

    # histório de mensagens
    for msg in st.session_state.qq_history:
        if msg["role"] == "user":
            st.markdown(
                f'<div style="display:flex;justify-content:flex-end;margin:6px 0">'
                f'<div style="background:#1A1C24;color:#F3F4F6;padding:10px 16px;'
                f'border-radius:16px 16px 4px 16px;font-size:13px;font-weight:600;'
                f'max-width:70%;font-family:Montserrat,sans-serif">{msg["content"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            # resposta do sistema
            r = msg.get("result")
            if r:
                _render_quote_response(r)
                _render_followup_questions(r)
            else:
                st.markdown(
                    f'<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:10px;'
                    f'padding:12px 16px;font-size:12px;color:#DC2626;font-family:Montserrat,sans-serif">'
                    f'{msg["content"]}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── Input — text_input + button (mais estável que st.form em tabs) ─────────
    col_inp, col_btn = st.columns([5, 1])
    with col_inp:
        user_input = st.text_input(
            "Sua consulta",
            placeholder='Ex: "Soja CIF Vietnam 25000 MT" ou "Acucar IC45 FOB Santos"',
            label_visibility="collapsed",
            key="qq_input_text",
        )
    with col_btn:
        enviado = st.button("Calcular", key="qq_btn_calcular", use_container_width=True)

    # dispara tanto no botão quanto no Enter (mudança de valor + state flag)
    _texto_atual = (user_input or "").strip()
    _processar = enviado and _texto_atual

    if _processar:
        texto = _texto_atual
        st.session_state.qq_history.append({"role": "user", "content": texto, "result": None})
        st.session_state.qq_input_text = ""   # limpa o campo

        with st.spinner("Calculando..."):
            try:
                parsed = _parse_query_gemini(texto)
                result = _calcular_quick_quote(parsed, snap)
            except Exception as _err:
                parsed = {}
                result = None
                st.session_state.qq_history.append({
                    "role": "system",
                    "content": f"Erro interno: {_err}",
                    "result": None,
                })
                st.session_state.qq_pending_result = None
                st.rerun()

        if result:
            st.session_state.qq_history.append({
                "role": "system",
                "content": "",
                "result": result,
            })
            st.session_state.qq_pending_result = result
        else:
            _prod_raw = parsed.get("produto") or "?"
            _inc_raw  = parsed.get("incoterm") or "?"
            _dst_raw  = parsed.get("destino")  or "?"
            st.session_state.qq_history.append({
                "role": "system",
                "content": (
                    f"Nao consegui calcular. Produto identificado: {_prod_raw} | "
                    f"Incoterm: {_inc_raw} | Destino: {_dst_raw}. "
                    f"Verifique e tente novamente. Ex: 'Soja CIF China 25000 MT'."
                ),
                "result": None,
            })
            st.session_state.qq_pending_result = None

        st.rerun()

    # ── Botoes de acao (aparece quando ha resultado recente) ─────────────────
    r = st.session_state.get("qq_pending_result")
    if r:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        st.markdown(
            '<div style="font-size:9px;font-weight:700;letter-spacing:1.5px;'
            'color:#6B7280;font-family:Montserrat,sans-serif;margin-bottom:8px">'
            'PROXIMOS PASSOS</div>',
            unsafe_allow_html=True,
        )
        ca, cb, cc, cd = st.columns(4)

        with ca:
            if st.button("Cotacao Completa", key="qq_act_form", use_container_width=True):
                # pre-preenche os campos do form de pricing
                prod_key = r.get("produto_key", "SOY")
                porto_lbl = r.get("porto_label", list(_PORTOS.keys())[0])
                st.session_state["px_prod"]  = list(_PRODUTOS.keys()).index(prod_key) \
                    if prod_key in _PRODUTOS else 0
                st.session_state["px_vol"]   = float(r["volume_mt"])
                st.session_state["qq_switch_to_form"] = True
                st.rerun()

        with cb:
            if st.button("Criar Deal", key="qq_act_deal", use_container_width=True):
                # salva no session state para o pipeline pegar
                st.session_state["new_deal_from_quote"] = {
                    "commodity":  r["produto_label"],
                    "price":      r.get("cif_usd") or r.get("fob_usd"),
                    "currency":   "USD",
                    "incoterm":   r["incoterm"],
                    "volume":     r["volume_mt"],
                    "destination": r["destino"],
                }
                st.session_state.current_view = "comercial"
                st.rerun()

        with cc:
            msg_comercial = _gerar_mensagem_comercial(r)
            st.download_button(
                "Msg WhatsApp",
                data=msg_comercial,
                file_name="indicativo_comercial.txt",
                mime="text/plain",
                key="qq_act_wpp",
                use_container_width=True,
            )

        with cd:
            if st.button("Limpar Chat", key="qq_act_clear", use_container_width=True):
                st.session_state.qq_history = []
                st.session_state.qq_pending_result = None
                st.rerun()
