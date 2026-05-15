# -*- coding: utf-8 -*-
"""
dashboards/cotacao_widget.py
============================
Aba de Cotação do Control Desk — Módulo Quote.

Arquitetura de dados:
  • CBOT ao vivo via yfinance:
      ZS=F  → Soja          ÷ 36.7437  → USD/MT
      ZM=F  → Farelo Soja   × 1.10231  → USD/MT  (USD/short ton → USD/MT)
      ZC=F  → Milho         ÷ 39.3680  → USD/MT
      SB=F  → Açúcar VHP    × 22.0462  → USD/MT
      IC45  → VHP + prêmio  (ajustável, padrão +100 USD/MT)
  • Basis via SAMBA_QUOTE API (GET /basis/ativo) com fallback operacional.
  • FOB = CBOT_USD_MT + Basis_USD_MT   (tudo em USD/MT, sem ¢/bu)
  • CIF = FOB + frete marítimo estimado
  • Comissão grãos  : USD 2,50/MT compra + USD 2,50/MT venda = USD 5,00/MT fixo
  • Comissão proteínas: % do contrato (1 / 1,5 / 2 %)

Seções:
  A · FRANGO CHINA  — 4 cortes, FCL-based, preços de mercado pré-carregados
  B · SUÍNA CHINA   — 7 cortes, FCL-based, preços de mercado pré-carregados
  C · GRÃOS EXPORT  — Soja · Farelo · Milho · Açúcar VHP · Açúcar IC45
"""
from __future__ import annotations

import os
import re
import sys
import time
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)

# ─── Constantes de conversão (idênticas ao SAMBA_LIMPO/MarketDataService) ─────
_BUSHEL_SOY_TO_MT         = 36.7437   # bu → MT  (soja grão)
_BUSHEL_CORN_TO_MT        = 39.3680   # bu → MT  (milho)
_SUGAR_CENTS_LB_TO_USD_MT = 22.0462   # ¢/lb → USD/MT  (açúcar VHP raw)
_SOYMEAL_STON_TO_MT       = 1.10231   # USD/short ton → USD/MT  (farelo soja)

# ─── API ──────────────────────────────────────────────────────────────────────
_QUOTE_API = os.getenv("SAMBA_QUOTE_API_URL", "http://localhost:8000")

# ─── Proteínas — CIF China 2025 (USD/MT) ──────────────────────────────────────
_FRANGO: list[dict] = [
    {"key": "patas", "name": "Patas de frango",           "price": 1750.0},
    {"key": "pes",   "name": "Pés de frango",             "price": 1650.0},
    {"key": "asa",   "name": "Asas MJW (mid-joint wing)", "price": 2500.0},
    {"key": "coxas", "name": "Coxas de frango",           "price": 1350.0},
]
_SUINA: list[dict] = [
    {"key": "pes_p", "name": "Pés de porco",     "price": 1500.0},
    {"key": "diant", "name": "Perna dianteira",  "price": 2000.0},
    {"key": "tras",  "name": "Perna traseira",   "price": 2900.0},
    {"key": "orel",  "name": "Orelhas de porco", "price": 1050.0},
    {"key": "cab",   "name": "Cabeça de porco",  "price":  750.0},
    {"key": "rabo",  "name": "Rabo de porco",    "price": 1950.0},
    {"key": "gord",  "name": "Gordura de porco", "price":  600.0},
]

# ─── Basis operacional por porto (USD/MT) — fallback quando API offline ────────
_BASIS_DEFAULTS: dict[str, dict[str, float]] = {
    "SOY":        {"PARANAGUA": -2.80, "SANTOS": -3.20, "BARCARENA": -1.60, "SALINOPOLIS": -1.80},
    "SOYMEAL":    {"PARANAGUA": -15.0, "SANTOS": -16.0, "BARCARENA": -12.0},
    "CORN":       {"PARANAGUA": -3.50, "SANTOS": -4.00, "BARCARENA": -2.50, "SALINOPOLIS": -2.80},
    "SUGAR_VHP":  {"SANTOS":   0.50, "PARANAGUA":  0.00, "BARCARENA":  0.30},
    "SUGAR_IC45": {"SANTOS":   0.50, "PARANAGUA":  0.00, "BARCARENA":  0.30},
}

# ─── Destinos CIF com frete Panamax/Supramax de referência (USD/MT) ────────────
_DESTINOS: dict[str, dict] = {
    # Ásia — Far East
    "China":            {"frete": 42.0, "grupo": "Ásia — Far East"},
    "Japão":            {"frete": 46.0, "grupo": "Ásia — Far East"},
    "Coreia do Sul":    {"frete": 44.0, "grupo": "Ásia — Far East"},
    "Taiwan":           {"frete": 44.0, "grupo": "Ásia — Far East"},
    # Ásia — Sudeste
    "Vietnã":           {"frete": 38.0, "grupo": "Ásia — Sudeste"},
    "Indonésia":        {"frete": 36.0, "grupo": "Ásia — Sudeste"},
    "Tailândia":        {"frete": 37.0, "grupo": "Ásia — Sudeste"},
    "Malásia":          {"frete": 36.0, "grupo": "Ásia — Sudeste"},
    "Filipinas":        {"frete": 38.0, "grupo": "Ásia — Sudeste"},
    "Myanmar":          {"frete": 39.0, "grupo": "Ásia — Sudeste"},
    # Ásia — Sul
    "Bangladesh":       {"frete": 40.0, "grupo": "Ásia — Sul"},
    "Paquistão":        {"frete": 36.0, "grupo": "Ásia — Sul"},
    "Índia":            {"frete": 35.0, "grupo": "Ásia — Sul"},
    "Sri Lanka":        {"frete": 36.0, "grupo": "Ásia — Sul"},
    # Oriente Médio
    "Arábia Saudita":   {"frete": 32.0, "grupo": "Oriente Médio"},
    "Emirados Árabes":  {"frete": 31.0, "grupo": "Oriente Médio"},
    "Egito":            {"frete": 29.0, "grupo": "Oriente Médio"},
    "Irã":              {"frete": 33.0, "grupo": "Oriente Médio"},
    "Iraque":           {"frete": 34.0, "grupo": "Oriente Médio"},
    "Omã":              {"frete": 32.0, "grupo": "Oriente Médio"},
    "Kuwait":           {"frete": 32.0, "grupo": "Oriente Médio"},
    # Europa
    "Espanha":          {"frete": 26.0, "grupo": "Europa"},
    "Países Baixos":    {"frete": 27.0, "grupo": "Europa"},
    "Alemanha":         {"frete": 28.0, "grupo": "Europa"},
    "Portugal":         {"frete": 25.0, "grupo": "Europa"},
    "Itália":           {"frete": 27.0, "grupo": "Europa"},
    "Turquia":          {"frete": 28.0, "grupo": "Europa"},
    "Reino Unido":      {"frete": 27.0, "grupo": "Europa"},
    "França":           {"frete": 26.0, "grupo": "Europa"},
    "Polônia":          {"frete": 28.0, "grupo": "Europa"},
    "Grécia":           {"frete": 27.0, "grupo": "Europa"},
    # África
    "Nigéria":          {"frete": 26.0, "grupo": "África"},
    "Marrocos":         {"frete": 26.0, "grupo": "África"},
    "Argélia":          {"frete": 27.0, "grupo": "África"},
    "Tunísia":          {"frete": 27.0, "grupo": "África"},
    "África do Sul":    {"frete": 28.0, "grupo": "África"},
    "Senegal":          {"frete": 24.0, "grupo": "África"},
    "Costa do Marfim":  {"frete": 24.0, "grupo": "África"},
    "Gana":             {"frete": 25.0, "grupo": "África"},
    "Angola":           {"frete": 25.0, "grupo": "África"},
    "Moçambique":       {"frete": 30.0, "grupo": "África"},
    "Tanzânia":         {"frete": 30.0, "grupo": "África"},
    "Quênia":           {"frete": 32.0, "grupo": "África"},
    # Américas
    "México":           {"frete": 22.0, "grupo": "Américas"},
    "Cuba":             {"frete": 22.0, "grupo": "Américas"},
    "Colômbia":         {"frete": 20.0, "grupo": "Américas"},
    "Venezuela":        {"frete": 20.0, "grupo": "Américas"},
    "Peru":             {"frete": 21.0, "grupo": "Américas"},
    "Chile":            {"frete": 21.0, "grupo": "Américas"},
    # Outros
    "Outros":           {"frete": 45.0, "grupo": "Outros"},
}

_GRAOS_ROTAS_SOJA = [
    "Sul/Sudeste — Paranaguá / Santos",
    "Arco Norte — Santarém / Barcarena / Itacoatiara",
]

# ─── Configuração por commodity ────────────────────────────────────────────────
_GRAOS_CFG: dict[str, dict] = {
    "Soja": {
        "commodity_api": "SOY",
        "market_key":    "soy_usd_mt",
        "porto_sul":     "PGROSSA",
        "porto_norte":   "BARCARENA",
        "hs":            "1201.90",
        "nota":          "CBOT ZS=F — ¢/bu ÷ 36.7437 → USD/MT",
        "has_arco_norte": True,
    },
    "Farelo de Soja": {
        "commodity_api": "SOYMEAL",
        "market_key":    "meal_usd_mt",
        "porto_sul":     "PGROSSA",
        "porto_norte":   "BARCARENA",
        "hs":            "2304.00",
        "nota":          "CBOT ZM=F — USD/short ton × 1.10231 → USD/MT",
        "has_arco_norte": True,
    },
    "Milho": {
        "commodity_api": "CORN",
        "market_key":    "corn_usd_mt",
        "porto_sul":     "PGROSSA",
        "porto_norte":   "BARCARENA",
        "hs":            "1005.90",
        "nota":          "CBOT ZC=F — ¢/bu ÷ 39.3680 → USD/MT",
        "has_arco_norte": True,
    },
    "Açúcar VHP": {
        "commodity_api": "SUGAR_VHP",
        "market_key":    "sug_usd_mt",
        "porto_sul":     "SANTOS",
        "porto_norte":   "SANTOS",
        "hs":            "1701.14",
        "nota":          "ICE SB=F — ¢/lb × 22.0462 → USD/MT",
        "has_arco_norte": False,
    },
    "Açúcar IC45": {
        "commodity_api": "SUGAR_IC45",
        "market_key":    "sug_usd_mt",   # base VHP + prêmio IC45
        "porto_sul":     "SANTOS",
        "porto_norte":   "SANTOS",
        "hs":            "1701.99",
        "nota":          "Derivado do VHP (SB=F) + prêmio IC45 ajustável. Ref.: ICE LIFFE White Sugar.",
        "has_arco_norte": False,
    },
}

# Mapeamento produto → código SAMBA_LIMPO
_PROD_CODE: dict[str, str] = {
    "Soja":           "SOY",
    "Farelo de Soja": "SOYMEAL",
    "Milho":          "CORN",
    "Açúcar VHP":     "VHP",
    "Açúcar IC45":    "IC45",
}

# Mapeamento destino → código SAMBA_LIMPO
_PAIS_CODE: dict[str, str] = {
    "China": "CHINA", "Japão": "JAPAN", "Coreia do Sul": "SOUTH_KOREA",
    "Taiwan": "TAIWAN", "Vietnã": "VIETNAM", "Indonésia": "INDONESIA",
    "Tailândia": "THAILAND", "Malásia": "MALAYSIA", "Filipinas": "PHILIPPINES",
    "Bangladesh": "BANGLADESH", "Paquistão": "PAKISTAN", "Índia": "INDIA",
    "Sri Lanka": "SRI_LANKA", "Myanmar": "MYANMAR",
    "Arábia Saudita": "SAUDI_ARABIA", "Emirados Árabes": "UAE",
    "Egito": "EGYPT", "Irã": "IRAN", "Iraque": "IRAQ",
    "Omã": "OMAN", "Kuwait": "KUWAIT",
    "Espanha": "SPAIN", "Países Baixos": "NETHERLANDS", "Alemanha": "GERMANY",
    "Portugal": "PORTUGAL", "Itália": "ITALY", "Turquia": "TURKEY",
    "Reino Unido": "UK", "França": "FRANCE", "Polônia": "POLAND", "Grécia": "GREECE",
    "Nigéria": "NIGERIA", "Marrocos": "MOROCCO", "Argélia": "ALGERIA",
    "Tunísia": "TUNISIA", "África do Sul": "SOUTH_AFRICA",
    "Senegal": "SENEGAL", "Costa do Marfim": "IVORY_COAST",
    "Gana": "GHANA", "Angola": "ANGOLA", "Moçambique": "MOZAMBIQUE",
    "Tanzânia": "TANZANIA", "Quênia": "KENYA",
    "México": "MEXICO", "Cuba": "CUBA", "Colômbia": "COLOMBIA",
    "Venezuela": "VENEZUELA", "Peru": "PERU", "Chile": "CHILE",
    "Outros": "CHINA",
}

# Porto de saída por (commodity, rota)
_PORTO_SAIDA: dict[tuple, str] = {
    ("Soja",           "Arco Norte"):  "BARCARENA",
    ("Soja",           "Sul/Sudeste"): "PARANAGUA",
    ("Farelo de Soja", "Arco Norte"):  "BARCARENA",
    ("Farelo de Soja", "Sul/Sudeste"): "PARANAGUA",
    ("Milho",          "Arco Norte"):  "BARCARENA",
    ("Milho",          "Sul/Sudeste"): "PARANAGUA",
    ("Açúcar VHP",     "Sul/Sudeste"): "SANTOS",
    ("Açúcar VHP",     "Arco Norte"):  "SANTOS",
    ("Açúcar IC45",    "Sul/Sudeste"): "SANTOS",
    ("Açúcar IC45",    "Arco Norte"):  "SANTOS",
}


# ─── CSS — Clean Light Mode · Paleta oficial Samba Export Brand Manual v1.0 ───
# Cores aprovadas: #FA8200 (Orange) · #262626 (Black 85%) · #7F7F7F (Black 50%)
#                  #BFBFBF (Black 25%) · #D9D9D9 (Black 15%) · #329632 (Green)
#                  #FA3232 (Red) · fundo branco · Montserrat em tudo
_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Montserrat:ital,wght@0,400;0,500;0,600;0,700;0,800&display=swap');

/* ══ TOKENS DA MARCA ══════════════════════════════════════════════════════════
   --samba-orange : #FA8200   primary accent
   --samba-text   : #262626   Black 85% — texto principal
   --samba-mid    : #7F7F7F   Black 50% — labels secundários
   --samba-muted  : #BFBFBF   Black 25% — placeholders, muted
   --samba-border : #D9D9D9   Black 15% — bordas e divisores
   --samba-bg     : #F7F7F8   fundo geral (quase branco)
   --samba-card   : #FFFFFF   fundo de card
   --samba-green  : #329632   positivo
   --samba-red    : #FA3232   alerta/negativo
   --samba-tint   : #FFF8F0   laranja tint (fundo CIF / destaque)
════════════════════════════════════════════════════════════════════════════ */
:root {
    --so: #FA8200; --st: #262626; --sm: #7F7F7F;
    --su: #BFBFBF; --sb: #D9D9D9; --sc: #FFFFFF;
    --sg: #329632; --sr: #FA3232; --sk: #FFF8F0;
}

/* ── Sub-tabs: FRANGO / SUÍNA / GRÃOS ─────────────────────────────────────── */
.st-key-cot_cat .stRadio > div {
    display:flex; gap:4px; flex-wrap:wrap;
    border-bottom:2px solid #D9D9D9;
    padding-bottom:0; margin-bottom:20px;
}
.st-key-cot_cat .stRadio > div label {
    flex:1; text-align:center; padding:10px 8px;
    font-size:11px; font-weight:700; letter-spacing:1.2px;
    cursor:pointer; color:#BFBFBF;
    border-bottom:3px solid transparent;
    background:transparent; border-radius:4px 4px 0 0;
    transition:all .15s; margin-bottom:-2px;
    font-family:'Montserrat',sans-serif;
}
.st-key-cot_cat .stRadio > div label:hover { color:#7F7F7F; }
.st-key-cot_cat .stRadio > div label:has(input:checked) {
    color:#FA8200 !important;
    border-bottom-color:#FA8200 !important;
    background:rgba(250,130,0,.05) !important;
}
.st-key-cot_cat .stRadio > div label input { display:none !important; }

/* ── Rota radio (Sul/Sudeste · Arco Norte) ─────────────────────────────────── */
.st-key-graos_rota_soja .stRadio > div { display:flex; flex-direction:row; gap:10px; }
.st-key-graos_rota_soja .stRadio > div label {
    flex:1; background:#FFFFFF;
    border:1.5px solid #D9D9D9;
    border-radius:8px; padding:10px 16px;
    font-size:12px; font-weight:600;
    color:#7F7F7F; cursor:pointer; transition:all .15s;
    text-align:center; box-shadow:none;
    font-family:'Montserrat',sans-serif;
}
.st-key-graos_rota_soja .stRadio > div label:hover {
    border-color:#FA8200; color:#FA8200;
}
.st-key-graos_rota_soja .stRadio > div label:has(input:checked) {
    border-color:#FA8200 !important; color:#FA8200 !important;
    background:#FFF8F0 !important;
    box-shadow:0 0 0 3px rgba(250,130,0,.12) !important;
}
.st-key-graos_rota_soja .stRadio > div label input { display:none !important; }

/* ── Inputs: text, number, selectbox — padrão unificado ────────────────────── */
div[data-testid="stTextInput"] input,
div[data-testid="stNumberInput"] input {
    background:#FFFFFF !important;
    color:#262626 !important;
    border:1.5px solid #D9D9D9 !important;
    border-radius:7px !important;
    font-size:13px !important;
    font-family:'Montserrat',sans-serif !important;
    padding:8px 12px !important;
    transition:border-color .15s, box-shadow .15s !important;
}
div[data-testid="stTextInput"] input:focus,
div[data-testid="stNumberInput"] input:focus {
    border-color:#FA8200 !important;
    box-shadow:0 0 0 3px rgba(250,130,0,.14) !important;
    outline:none !important;
}
div[data-testid="stTextInput"] label,
div[data-testid="stNumberInput"] label {
    font-size:10px !important; font-weight:700 !important;
    letter-spacing:1.2px !important; color:#7F7F7F !important;
    text-transform:uppercase !important;
    font-family:'Montserrat',sans-serif !important;
}

/* ── Selectbox ─────────────────────────────────────────────────────────────── */
div[data-testid="stSelectbox"] > div > div {
    background:#FFFFFF !important;
    border:1.5px solid #D9D9D9 !important;
    border-radius:7px !important;
    color:#262626 !important;
    font-family:'Montserrat',sans-serif !important;
    font-size:13px !important;
}
div[data-testid="stSelectbox"] > div > div:focus-within {
    border-color:#FA8200 !important;
    box-shadow:0 0 0 3px rgba(250,130,0,.14) !important;
}
div[data-testid="stSelectbox"] label {
    font-size:10px !important; font-weight:700 !important;
    letter-spacing:1.2px !important; color:#7F7F7F !important;
    text-transform:uppercase !important;
    font-family:'Montserrat',sans-serif !important;
}

/* ── Section title ─────────────────────────────────────────────────────────── */
.cot-card-title {
    font-size:9px; letter-spacing:2.2px; font-weight:800;
    color:#FA8200; text-transform:uppercase;
    margin:4px 0 14px; display:flex; align-items:center; gap:6px;
    font-family:'Montserrat',sans-serif;
}
.cot-divider { height:1px; background:#D9D9D9; margin:14px 0; opacity:.5; }

/* ── Live badge ────────────────────────────────────────────────────────────── */
.live-badge {
    display:inline-flex; align-items:center; gap:5px;
    font-size:10px; font-weight:700; letter-spacing:1px;
    padding:3px 10px; border-radius:20px; margin-bottom:14px;
    font-family:'Montserrat',sans-serif;
}
.live-badge.green  { background:#EDF7ED; color:#329632; border:1px solid rgba(50,150,50,.3); }
.live-badge.yellow { background:#FFFBE6; color:#8C6200;   border:1px solid rgba(180,130,0,.3); }

/* ── Price stack ───────────────────────────────────────────────────────────── */
.pstack {
    background:#FFFFFF; border:1.5px solid #D9D9D9;
    border-radius:10px; overflow:hidden;
}
.pstack-row {
    display:flex; justify-content:space-between; align-items:center;
    padding:10px 16px; border-bottom:1px solid #D9D9D9;
    font-size:13px; font-family:'Montserrat',sans-serif;
}
.pstack-row:last-child { border-bottom:none; }
.pstack-row .pl { color:#7F7F7F; }
.pstack-row .pv { font-weight:700; color:#262626; }
/* CIF row no price stack: dark hero (Brand pág. 3) */
.pstack-row.cif-row { background:#262626 !important; border-bottom-color:#262626 !important; padding:12px 14px !important; }
.pstack-row.cif-row .pl { color:#BFBFBF !important; font-weight:700 !important; letter-spacing:.8px; text-transform:uppercase; font-size:11px !important; }
.pstack-row.cif-row .pv { color:#FA8200 !important; font-size:18px !important; font-weight:900 !important; font-family:'Montserrat',sans-serif; }

/* ── Metric grid ───────────────────────────────────────────────────────────── */
.met-grid {
    display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
    gap:10px; margin:16px 0;
}
/* FOB / Volume / Total — branco com borda-esquerda 3px laranja */
.met-box {
    background:#FFFFFF; border:1px solid #E8E9EC;
    border-left:3px solid #FA8200;
    border-radius:8px; padding:14px 16px;
    box-shadow: 0 1px 3px rgba(0,0,0,.04);
    transition: box-shadow .15s;
}
.met-box:hover { box-shadow: 0 2px 8px rgba(0,0,0,.07); }
/* CIF — hero metric: dark #262626 com valor em #FA8200 */
.met-box.cif-hero {
    background:#262626 !important; border:1px solid #262626 !important;
    border-left:3px solid #FA8200 !important;
    box-shadow: 0 4px 14px rgba(0,0,0,.20);
}
.met-box.cif-hero .met-lbl { color:#BFBFBF !important; }
.met-box.cif-hero .met-val { color:#FA8200 !important; font-weight:900 !important; }
.met-box.cif-hero .met-sub { color:#7F7F7F !important; }
.met-lbl {
    font-size:9px; letter-spacing:1.5px; color:#7F7F7F;
    font-weight:700; text-transform:uppercase; margin-bottom:5px;
    font-family:'Montserrat',sans-serif;
}
.met-val { font-size:19px; font-weight:800; color:#262626; font-family:'Montserrat',sans-serif; }
.met-val.gold { color:#FA8200; }
.met-sub { font-size:10px; color:#BFBFBF; margin-top:3px; font-family:'Montserrat',sans-serif; }

/* ── Commission box ────────────────────────────────────────────────────────── */
.comm-box {
    background:#FFFFFF; border:1.5px solid #D9D9D9;
    border-radius:10px; padding:14px 18px;
}
.comm-row {
    display:flex; justify-content:space-between; align-items:center;
    padding:7px 0; font-size:13px;
    border-bottom:1px solid #D9D9D9;
    font-family:'Montserrat',sans-serif;
}
.comm-row:last-child { border-bottom:none; font-weight:700; }
.comm-row .cl { color:#7F7F7F; }
.comm-row .cv { font-weight:600; color:#262626; }
.comm-row.hi .cv { color:#FA8200; font-weight:700; font-size:14px; }

/* ── Product table (proteínas) ─────────────────────────────────────────────── */
.pt-header {
    display:grid; gap:4px; padding:6px 0 8px;
    border-bottom:1px solid #D9D9D9;
    font-size:9px; font-weight:700; letter-spacing:1.2px;
    color:#BFBFBF; text-transform:uppercase;
    font-family:'Montserrat',sans-serif;
}
.pt-row {
    display:grid; gap:4px; padding:8px 0;
    border-bottom:1px solid rgba(217,217,217,.5);
    font-size:13px; align-items:center;
    color:#262626; font-family:'Montserrat',sans-serif;
}
.pt-row:last-child { border-bottom:none; }
.pt-total {
    display:grid; gap:4px; padding:8px 0;
    border-top:2px solid #D9D9D9;
    font-size:13px; font-weight:700; align-items:center;
    color:#262626; font-family:'Montserrat',sans-serif;
}

/* ── Info boxes ────────────────────────────────────────────────────────────── */
.arco-box {
    font-size:11px; color:#7F5200;
    background:#FFF8E6; border:1px solid #E6C87A;
    border-radius:7px; padding:9px 14px; margin:8px 0;
    font-family:'Montserrat',sans-serif;
}
.ic45-box {
    font-size:11px; color:#204E70;
    background:#EDF4FB; border:1px solid #A8C8E8;
    border-radius:7px; padding:9px 14px; margin:8px 0;
    font-family:'Montserrat',sans-serif;
}

/* ── Footer note ───────────────────────────────────────────────────────────── */
.cot-note {
    font-size:11px; color:#BFBFBF;
    margin-top:14px; line-height:1.7;
    font-family:'Montserrat',sans-serif;
}

/* ── Result box ────────────────────────────────────────────────────────────── */
.cot-result {
    background:#FFF8F0; border:1.5px solid rgba(250,130,0,.35);
    border-radius:10px; padding:16px 20px; margin-top:14px;
    font-family:'Montserrat',sans-serif;
}
.cot-result a { color:#FA8200; font-weight:700; text-decoration:none; }

/* ── Botão GERAR — laranja sólido · Montserrat Bold ───────────────────────── */
/* Cobre ambas as variantes de comm_type (USD_TON e PERCENT)                   */
[class*="st-key-cot_gerar_btn"] button,
.st-key-cot_samba_btn button {
    background:#FA8200 !important; color:#FFFFFF !important;
    border:none !important; border-radius:8px !important;
    font-weight:700 !important; letter-spacing:1px !important;
    font-size:12px !important; width:100% !important;
    font-family:'Montserrat',sans-serif !important;
    transition:background .15s !important;
    padding:10px 0 !important;
}
[class*="st-key-cot_gerar_btn"] button:hover,
.st-key-cot_samba_btn button:hover { background:#C86600 !important; }

/* ── SAMBA Engine result box ───────────────────────────────────────────────── */
.samba-box {
    background:#FFF8F0; border:1.5px solid rgba(250,130,0,.22);
    border-radius:10px; overflow:hidden; margin-top:12px;
}
.samba-box-title {
    background:rgba(250,130,0,.09); padding:10px 16px;
    font-size:9px; font-weight:700; letter-spacing:2px;
    color:#FA8200; text-transform:uppercase;
    font-family:'Montserrat',sans-serif;
}
</style>
"""


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _fmt(n: float) -> str:
    return f"{n:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt2(n: float, d: int = 2) -> str:
    return f"{n:,.{d}f}".replace(",", "X").replace(".", ",").replace("X", ".")

def _section(title: str, icon: str = "") -> None:
    prefix = f"{icon} " if icon else ""
    st.markdown(
        f'<div class="cot-card-title">{prefix}{title}</div>',
        unsafe_allow_html=True,
    )

def _divider() -> None:
    st.markdown('<div class="cot-divider"></div>', unsafe_allow_html=True)


# ─── Fetch CBOT ao vivo ────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _fetch_market() -> dict:
    """Busca CBOT em USD/MT. Idêntico ao MarketDataService do SAMBA_LIMPO."""
    try:
        import yfinance as yf

        def _close(sym: str) -> Optional[float]:
            try:
                d = yf.Ticker(sym).history(period="2d")
                return float(d["Close"].iloc[-1]) if not d.empty else None
            except Exception:
                return None

        def _norm(v: Optional[float]) -> Optional[float]:
            """¢/bu → USD/bu (ZS=F e ZC=F chegam em cents)."""
            if v is None:
                return None
            return v / 100.0 if v > 100 else v

        soy_raw   = _close("ZS=F")
        meal_raw  = _close("ZM=F")   # USD/short ton
        corn_raw  = _close("ZC=F")
        sugar_raw = _close("SB=F")   # ¢/lb
        usd_brl   = _close("USDBRL=X")

        soy_mt  = (_norm(soy_raw)  or 0.0) * _BUSHEL_SOY_TO_MT
        meal_mt = (meal_raw or 0.0) * _SOYMEAL_STON_TO_MT
        corn_mt = (_norm(corn_raw) or 0.0) * _BUSHEL_CORN_TO_MT
        sug_mt  = (sugar_raw or 0.0) * _SUGAR_CENTS_LB_TO_USD_MT

        ok = soy_mt > 50 and corn_mt > 50
        return {
            "ok":          ok,
            "soy_usd_mt":  round(soy_mt,  2),
            "meal_usd_mt": round(meal_mt, 2),
            "corn_usd_mt": round(corn_mt, 2),
            "sug_usd_mt":  round(sug_mt,  2),
            "usd_brl":     round(usd_brl or 5.85, 4),
            "ts":          time.strftime("%H:%M"),
        }
    except Exception as exc:
        logger.warning(f"yfinance falhou: {exc}")
        return {
            "ok":          False,
            "soy_usd_mt":  365.0,
            "meal_usd_mt": 385.0,
            "corn_usd_mt": 175.0,
            "sug_usd_mt":  380.0,
            "usd_brl":     5.85,
            "ts":          "fallback",
        }


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_basis(commodity: str, porto: str) -> float:
    """Basis via API → fallback para _BASIS_DEFAULTS."""
    try:
        import requests
        r = requests.get(
            f"{_QUOTE_API}/basis/ativo",
            params={"commodity": commodity, "porto": porto},
            timeout=2.0,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("status") != "not_found" and "basis" in data:
                return float(data["basis"])
    except Exception:
        pass
    return _BASIS_DEFAULTS.get(commodity, {}).get(porto, 0.0)


# ─── SAMBA Engine ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=120, show_spinner=False)
def _call_samba_quote(
    produto_code: str, volume: float, incoterm: str,
    pais_destino: str, porto_saida: str,
    comm_usd_mt: float = 5.0, tipo_contrato: str = "SPOT", meses: int = 1,
) -> dict | None:
    try:
        import requests
        payload = {
            "produto":          produto_code,
            "volume":           volume,
            "incoterm":         incoterm,
            "modo":             "EXPORTACAO",
            "pais_destino":     pais_destino,
            "porto_saida":      porto_saida,
            "comissao_valor":   comm_usd_mt,
            "comissao_tipo":    "USD_TON",
            "tipo_contrato":    tipo_contrato,
            "meses_contrato":   meses,
            "incluir_impostos": True,
        }
        r = requests.post(f"{_QUOTE_API}/quote/simulate", json=payload, timeout=12.0)
        if r.status_code == 200:
            return r.json()
        logger.warning(f"SAMBA API {r.status_code}: {r.text[:150]}")
    except Exception as exc:
        logger.info(f"SAMBA Quote API indisponível: {exc}")
    return None


def _render_samba_stack(sim: dict) -> None:
    campiao = sim.get("campiao") or sim.get("champion") or {}
    if not campiao:
        st.warning("Simulação sem resultado de campeão.")
        return

    fob_mkt      = campiao.get("preco_fob_mercado_usd_mt", 0.0)
    cif_mkt      = campiao.get("cif_mercado_usd_mt", 0.0)
    frete_int    = campiao.get("frete_interno_usd_mt", 0.0)
    elevacao     = campiao.get("elevacao_portuaria_usd_mt", 0.0)
    frete_mar_bl = campiao.get("frete_maritimo_blindado_usd_mt",
                               campiao.get("frete_maritimo_usd_mt", 0.0))
    geo_prem     = campiao.get("geopolitcs_premium_applied_usd", 0.0)
    demurrage    = campiao.get("demurrage_usd_mt", 0.0)
    margem       = campiao.get("margem_cif_usd_mt", 0.0)
    navio        = campiao.get("navio_otimizado", "—")
    terminal     = campiao.get("terminal_nome", "—")
    rota_int     = campiao.get("rota_interna_detalhe", "—")
    porto_saida  = campiao.get("porto_saida", "—")
    porto_dest   = campiao.get("porto_destino", "—")
    geo_zone     = campiao.get("geopolitcs_zone", "Segura")
    geo_alert    = campiao.get("geopolitcs_alert", "")
    dem_alert    = campiao.get("demurrage_alert", "")
    margem_color = "#329632" if margem >= 0 else "#D93025"

    st.markdown(f"""
<div class="samba-box">
  <div class="samba-box-title">SAMBA Engine — Simulação Completa</div>
  <div class="pstack" style="border:none;border-radius:0;box-shadow:none">
    <div class="pstack-row"><span class="pl">FOB Mercado (CBOT + Basis)</span><span class="pv">USD {_fmt2(fob_mkt)}/MT</span></div>
    <div class="pstack-row"><span class="pl">Frete Interno · {rota_int[:50]}</span><span class="pv">+ USD {_fmt2(frete_int)}/MT</span></div>
    <div class="pstack-row"><span class="pl">Elevação Portuária · {terminal[:35]}</span><span class="pv">+ USD {_fmt2(elevacao)}/MT</span></div>
    <div class="pstack-row"><span class="pl">Frete Marítimo · {navio} · {porto_saida} → {porto_dest}</span><span class="pv">+ USD {_fmt2(frete_mar_bl)}/MT</span></div>
    <div class="pstack-row">
      <span class="pl">Risco Geopolítico ({geo_zone})</span>
      <span class="pv" style="color:{'#FA8200' if geo_prem > 0 else '#BFBFBF'}">+ USD {_fmt2(geo_prem)}/MT</span>
    </div>
    <div class="pstack-row">
      <span class="pl">Demurrage</span>
      <span class="pv" style="color:{'#B07800' if demurrage > 0 else '#BFBFBF'}">+ USD {_fmt2(demurrage)}/MT</span>
    </div>
    <div class="pstack-row cif-row"><span class="pl">CIF Destino</span><span class="pv">USD {_fmt2(cif_mkt)}/MT</span></div>
    <div class="pstack-row"><span class="pl">Margem Estimada Origem</span>
      <span class="pv" style="color:{margem_color}">USD {_fmt2(margem)}/MT</span></div>
  </div>
</div>
""", unsafe_allow_html=True)

    if geo_alert:
        st.markdown(
            f'<div style="font-size:11px;color:#7F5200;background:#FFF8E6;'
            f'border:1px solid #FFD980;border-radius:7px;padding:7px 12px;margin-top:6px;'
            f'font-family:Montserrat,sans-serif">'
            f'⚠ Geopolítico: {geo_alert}</div>', unsafe_allow_html=True)
    if dem_alert:
        st.markdown(
            f'<div style="font-size:11px;color:#7F5200;background:#FFFBF0;'
            f'border:1px solid rgba(180,130,0,.30);border-radius:7px;padding:7px 12px;margin-top:4px;'
            f'font-family:Montserrat,sans-serif">'
            f'⚓ Demurrage: {dem_alert}</div>', unsafe_allow_html=True)

    podio = sim.get("podio", [])
    if podio:
        st.markdown(
            '<div style="font-size:9px;letter-spacing:2px;font-weight:700;'
            'color:#BFBFBF;text-transform:uppercase;margin:14px 0 6px;'
            'font-family:Montserrat,sans-serif">Rotas Alternativas</div>',
            unsafe_allow_html=True,
        )
        for i, alt in enumerate(podio[:3], start=2):
            cif_a  = alt.get("cif_mercado_usd_mt", 0.0)
            rota_a = alt.get("porto_saida", "—")
            st.markdown(
                f'<div style="font-size:12px;color:#7F7F7F;padding:6px 0;'
                f'border-bottom:1px solid #F4F4F4;font-family:Montserrat,sans-serif">'
                f'<span style="color:#FA8200;font-weight:700">#{i}</span>  '
                f'CIF <span style="font-weight:700;color:#1A1A1A">USD {_fmt2(cif_a)}/MT</span>  ·  {rota_a}</div>',
                unsafe_allow_html=True,
            )


# ─── Proteínas (frango e suína compartilham o engine) ─────────────────────────

def _render_proteina(cat_key: str, products: list[dict],
                     default_fcl: int, label: str, note: str) -> None:

    # ── Parâmetros + Destino CIF ───────────────────────────────────────────────
    _section("PARÂMETROS DO CONTRATO", "⚙")
    p1, p2, p3, p4 = st.columns([1.2, 1.2, 0.9, 2.2])
    fcl_weight = p1.number_input("Peso por FCL (MT)", 18.0, 28.0, 24.0, 0.5,
                                  key=f"cot_fw_{cat_key}")
    fcl_month  = p2.number_input("FCL / mês por produto", 1, 200, default_fcl, 1,
                                  key=f"cot_fm_{cat_key}")
    months     = p3.number_input("Duração (meses)", 1, 24, 12, 1,
                                  key=f"cot_mo_{cat_key}")
    destino    = p4.selectbox(
        "Destino CIF",
        list(_DESTINOS.keys()),
        index=0,
        key=f"cot_dest_{cat_key}",
    )
    frete_dest = _DESTINOS.get(destino, {}).get("frete", 42.0)

    st.markdown('<hr style="border:none;border-top:1px solid #F0F0F0;margin:16px 0 14px">', unsafe_allow_html=True)

    # ── Tabela de preços + toggle Incluir ─────────────────────────────────────
    _section(f"PREÇOS CIF {destino.upper()} — {label}", "📦")

    # Cabeçalho: ✓ | Produto | USD/MT | MT/mês | Receita/mês | Total contrato
    h0, h1, h2, h3, h4, h5 = st.columns([0.45, 2.8, 1.4, 1.2, 1.9, 2.0])
    for col, lbl in zip(
        [h0, h1, h2, h3, h4, h5],
        ["", "Produto", "USD / MT", "MT / mês", "Receita / mês", "Total contrato"],
    ):
        col.markdown(
            f'<div style="font-size:9px;font-weight:700;letter-spacing:1.1px;color:#BFBFBF;'
            f'padding:4px 0 8px;border-bottom:1px solid #E8E9EC;text-transform:uppercase">{lbl}</div>',
            unsafe_allow_html=True,
        )

    tot_mt = tot_month = tot_total = 0.0
    prices_snap: list[dict] = []
    n_incluidos = 0

    for p in products:
        c0, c1, c2, c3, c4, c5 = st.columns([0.45, 2.8, 1.4, 1.2, 1.9, 2.0])

        # Toggle Incluir (checkbox compacto)
        incluir = c0.checkbox(
            "inc",
            value=True,
            key=f"cot_inc_{cat_key}_{p['key']}",
            label_visibility="collapsed",
        )

        text_color = "#1A1A1A" if incluir else "#BFBFBF"
        strike     = "text-decoration:line-through;" if not incluir else ""
        c1.markdown(
            f'<div style="font-size:13px;color:{text_color};padding:6px 0;'
            f'border-bottom:1px solid #F4F4F4;font-family:Montserrat,sans-serif;{strike}">'
            f'{p["name"]}</div>',
            unsafe_allow_html=True,
        )
        price = c2.number_input(
            "Preço", min_value=100.0, max_value=15000.0, step=50.0,
            value=float(p["price"]),
            key=f"cot_price_{cat_key}_{p['key']}",
            label_visibility="collapsed",
            disabled=not incluir,
        )

        mt    = float(fcl_weight) * int(fcl_month)
        month = mt * price
        total = month * int(months)

        if incluir:
            prices_snap.append({
                "name":  p["name"],
                "price": float(price),
                "mt_month": mt,
                "total": total,
            })
            tot_mt    += mt
            tot_month += month
            tot_total += total
            n_incluidos += 1

        dim_color = "#BFBFBF" if not incluir else None
        for col, txt, color in [
            (c3, f"{_fmt(mt)} MT",   dim_color or "#7F7F7F"),
            (c4, f"$ {_fmt(month)}", dim_color or "#FA8200"),
            (c5, f"$ {_fmt(total)}", dim_color or "#1A1A1A"),
        ]:
            col.markdown(
                f'<div style="font-size:13px;color:{color};font-family:Montserrat,sans-serif;'
                f'font-weight:600;padding:6px 0;border-bottom:1px solid #F4F4F4;'
                f'text-align:right">{txt}</div>',
                unsafe_allow_html=True,
            )

    # Linha de totais (apenas produtos incluídos)
    t0, t1, _, t3, t4, t5 = st.columns([0.45, 2.8, 1.4, 1.2, 1.9, 2.0])
    t1.markdown(
        f'<div style="font-size:9px;letter-spacing:1px;color:#7F7F7F;font-weight:700;'
        f'padding:8px 0 6px;border-top:2px solid #E8E9EC;font-family:Montserrat,sans-serif;'
        f'text-transform:uppercase">TOTAL · {n_incluidos} produto{"s" if n_incluidos != 1 else ""}</div>',
        unsafe_allow_html=True,
    )
    for col, txt, color in [
        (t3, f"{_fmt(tot_mt)} MT",   "#1A1A1A"),
        (t4, f"$ {_fmt(tot_month)}", "#FA8200"),
        (t5, f"$ {_fmt(tot_total)}", "#1A1A1A"),
    ]:
        col.markdown(
            f'<div style="font-size:13px;color:{color};font-family:Montserrat,sans-serif;font-weight:700;'
            f'padding:8px 0 6px;border-top:2px solid #E8E9EC;text-align:right">{txt}</div>',
            unsafe_allow_html=True,
        )

    # ── Métricas ───────────────────────────────────────────────────────────────
    tot_fcl = n_incluidos * int(fcl_month) * int(months)
    st.markdown(f"""
<div class="met-grid" style="margin-top:18px">
  <div class="met-box">
    <div class="met-lbl">Volume Mensal</div>
    <div class="met-val">{_fmt(tot_mt)} MT</div>
    <div class="met-sub">{n_incluidos * int(fcl_month)} FCL / mês ({n_incluidos} cortes)</div>
  </div>
  <div class="met-box cif-hero">
    <div class="met-lbl">Receita Mensal</div>
    <div class="met-val">$ {_fmt(tot_month)}</div>
    <div class="met-sub">CIF {destino}</div>
  </div>
  <div class="met-box">
    <div class="met-lbl">Total do Contrato</div>
    <div class="met-val">$ {_fmt(tot_total)}</div>
    <div class="met-sub">{int(months)} meses · {tot_fcl} FCL</div>
  </div>
  <div class="met-box">
    <div class="met-lbl">Volume Total</div>
    <div class="met-val">{_fmt(tot_mt * int(months))} MT</div>
    <div class="met-sub">ao longo do contrato</div>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Comissão — % sobre contrato ────────────────────────────────────────────
    st.markdown('<hr style="border:none;border-top:1px solid #F0F0F0;margin:20px 0 14px">', unsafe_allow_html=True)
    _section("COMISSÃO ESTIMADA", "💰")
    c1v, c2v, c3v = tot_total * 0.01, tot_total * 0.015, tot_total * 0.02
    c3m = tot_month * 0.02
    st.markdown(f"""
<div class="comm-box">
  <div class="comm-row"><span class="cl">1% — mínimo</span><span class="cv">$ {_fmt(c1v)}</span></div>
  <div class="comm-row"><span class="cl">1,5% — referência de mercado</span><span class="cv">$ {_fmt(c2v)}</span></div>
  <div class="comm-row"><span class="cl">2% — máximo usual</span><span class="cv">$ {_fmt(c3v)}</span></div>
  <div class="comm-row hi"><span class="cl">A receber / mês (base 2%)</span><span class="cv">$ {_fmt(c3m)}</span></div>
</div>
""", unsafe_allow_html=True)

    comm_opts = {"1% (mínimo)": 0.01, "1,5% (referência)": 0.015, "2% (máximo)": 0.02}
    chosen    = st.selectbox("Alíquota para cotação formal", list(comm_opts.keys()),
                             index=1, key=f"cot_comm_{cat_key}")
    comm_rate = comm_opts[chosen]

    st.markdown(f'<p class="cot-note">{note}</p>', unsafe_allow_html=True)

    if not prices_snap:
        st.info("Selecione ao menos um produto para gerar cotação.")
        return

    _render_formal_quote_form(
        category=f"{label} — CIF {destino.upper()}",
        products_snapshot=prices_snap,
        fcl_weight=float(fcl_weight), fcl_month=int(fcl_month), months=int(months),
        tot_mt=tot_mt, tot_month=tot_month, tot_total=tot_total,
        comm_rate=comm_rate, comm_type="PERCENT",
        extra={"destino": destino, "frete_ref_usd_mt": frete_dest},
    )


def _render_frango() -> None:
    _render_proteina(
        cat_key="frango", products=_FRANGO, default_fcl=80, label="FRANGO",
        note=(
            "⚠ Estimativa de referência 2025. Patas e pés têm alta demanda cultural na China. "
            "MJW é o corte premium de asa. Preços variam por câmbio, frigorífico habilitado "
            "pelo MAPA/GACC e sazonalidade. FCL = reefer 40' HC congelado."
        ),
    )

def _render_suina() -> None:
    _render_proteina(
        cat_key="suina", products=_SUINA, default_fcl=10, label="SUÍNA",
        note=(
            "⚠ Estimativa de referência 2025. Preços variam por câmbio, demanda sazonal, "
            "habilitação do frigorífico e negociação. Fontes: ABPA, MAPA, cotações de exportação. "
            "FCL = reefer 40' HC. Peso por FCL: 22–27 MT conforme produto e embalagem."
        ),
    )


# ─── Grãos ────────────────────────────────────────────────────────────────────

def _render_graos() -> None:
    mkt = _fetch_market()

    # Badge ao vivo
    badge_cls = "green" if mkt["ok"] else "yellow"
    badge_txt = f"AO VIVO · {mkt['ts']}" if mkt["ok"] else "FALLBACK OPERACIONAL"
    st.markdown(
        f'<span class="live-badge {badge_cls}">● {badge_txt}</span>',
        unsafe_allow_html=True,
    )

    # ── Parâmetros principais ──────────────────────────────────────────────────
    _section("COMMODITY & DESTINO", "🌾")

    col_prod, col_dest, col_vol = st.columns([1.5, 2, 1.5])
    commodity = col_prod.selectbox(
        "Commodity", list(_GRAOS_CFG.keys()), key="cot_graos_prod",
    )
    destino = col_dest.selectbox(
        "Destino CIF", list(_DESTINOS.keys()), key="cot_graos_dest",
    )
    volume = col_vol.number_input(
        "Volume (MT)", 100.0, 500_000.0, 50_000.0, 500.0, key="cot_graos_vol",
    )

    cfg = _GRAOS_CFG[commodity]

    # ── Rota de saída ──────────────────────────────────────────────────────────
    rota = _GRAOS_ROTAS_SOJA[0]   # padrão Sul/Sudeste
    if cfg["has_arco_norte"]:
        _divider()
        _section("ROTA DE SAÍDA")
        rota = st.radio(
            "Rota de saída", _GRAOS_ROTAS_SOJA, index=0,
            key="graos_rota_soja", horizontal=True,
            label_visibility="collapsed",
        )
        if "Arco Norte" in rota:
            st.markdown(
                '<div class="arco-box">⚡ <b>Arco Norte</b> — frete interno menor, maior '
                'competitividade CIF destino. Portos: Santarém (PA) · Barcarena (PA) · '
                'Itacoatiara (AM) · Miritituba (PA). Basis diferenciado: menor custo de origem.</div>',
                unsafe_allow_html=True,
            )

    # IC45 — prêmio ajustável
    ic45_premium = 0.0
    if commodity == "Açúcar IC45":
        _divider()
        st.markdown(
            '<div class="ic45-box">ℹ️ <b>Açúcar IC45</b> — ICUMSA ≤ 45 (branco refinado). '
            'Cotado como prêmio sobre o VHP (SB=F ICE). Referência: ICE LIFFE White Sugar. '
            'O prêmio IC45/VHP varia tipicamente entre USD 80–130/MT.</div>',
            unsafe_allow_html=True,
        )
        ic45_premium = float(
            st.slider("Prêmio IC45 sobre VHP (USD/MT)", 50.0, 200.0, 100.0, 5.0,
                      key="cot_ic45_premium")
        )

    st.markdown('<hr style="border:none;border-top:1px solid #F0F0F0;margin:16px 0 14px">', unsafe_allow_html=True)

    # ── Determina porto de referência para basis ───────────────────────────────
    rota_key   = "Arco Norte" if "Arco Norte" in rota else "Sul/Sudeste"
    porto_ref  = cfg["porto_norte"] if "Arco Norte" in rota else cfg["porto_sul"]
    porto_api  = _PORTO_SAIDA.get((commodity, rota_key), "PARANAGUA")
    prod_api   = _PROD_CODE.get(commodity, "SOY")
    pais_api   = _PAIS_CODE.get(destino, "CHINA")

    # Preço base ao vivo
    cbot_base = mkt.get(cfg["market_key"], 365.0) or 365.0
    if commodity == "Açúcar IC45":
        cbot_base = cbot_base + ic45_premium   # VHP + prêmio IC45

    # Basis — key inclui porto_ref para forçar reset quando a rota muda
    basis_live = _fetch_basis(cfg["commodity_api"], porto_ref)

    # ── Composição de preço ────────────────────────────────────────────────────
    _section("COMPOSIÇÃO DE PREÇO — USD/MT", "📊")

    col_nota = {
        "Soja":           "CBOT ZS=F",
        "Farelo de Soja": "CBOT ZM=F",
        "Milho":          "CBOT ZC=F",
        "Açúcar VHP":     "ICE SB=F",
        "Açúcar IC45":    f"ICE VHP + prêmio IC45",
    }.get(commodity, "CBOT")

    r1, r2, r3, r4 = st.columns(4)

    # CBOT: chave inclui commodity para reset quando commodity muda
    # Bucket de 10 USD/MT: quando o CBOT ao vivo muda ≥10 pontos,
    # a key muda → widget reseta para o valor ao vivo automaticamente.
    _cbot_bucket = int(cbot_base // 10) * 10
    cbot_inp = r1.number_input(
        f"Base ({col_nota}) USD/MT", 50.0, 2000.0,
        float(round(cbot_base, 1)), 1.0,
        key=f"cot_graos_cbot_{commodity}_{_cbot_bucket}",
    )
    # Basis: chave inclui porto_ref para reset quando rota/porto muda
    basis_inp = r2.number_input(
        f"Basis Porto ({porto_ref}) USD/MT", -80.0, 80.0,
        float(round(basis_live, 2)), 0.5,
        key=f"cot_graos_basis_{porto_ref}_{commodity}",
    )
    frete_def = _DESTINOS.get(destino, {}).get("frete", 42.0)
    frete_inp = r3.number_input(
        "Frete Marítimo USD/MT", 10.0, 150.0,
        frete_def, 0.5,
        key=f"cot_graos_frete_{destino}",
    )
    cambio = r4.number_input(
        "Câmbio BRL/USD", 4.0, 10.0,
        float(mkt.get("usd_brl", 5.85)), 0.05,
        key="cot_graos_fx",
    )

    fob_est = cbot_inp + basis_inp
    cif_est = fob_est + frete_inp
    val_tot = cif_est * float(volume)

    # Price stack
    st.markdown(f"""
<div class="pstack" style="margin-top:14px">
  <div class="pstack-row">
    <span class="pl">{col_nota} (referência ao vivo)</span>
    <span class="pv">USD {_fmt2(cbot_inp)} / MT</span>
  </div>
  <div class="pstack-row">
    <span class="pl">Basis · Porto {porto_ref}</span>
    <span class="pv">{'+' if basis_inp >= 0 else ''}{_fmt2(basis_inp)} / MT</span>
  </div>
  <div class="pstack-row" style="background:#F9F9FB">
    <span class="pl" style="font-weight:600;color:#1A1A1A">FOB Estimado</span>
    <span class="pv" style="color:#1A1A1A;font-size:14px">USD {_fmt2(fob_est)} / MT</span>
  </div>
  <div class="pstack-row">
    <span class="pl">Frete Marítimo · {destino}</span>
    <span class="pv">+ USD {_fmt2(frete_inp)} / MT</span>
  </div>
  <div class="pstack-row cif-row">
    <span class="pl">CIF {destino}</span>
    <span class="pv">USD {_fmt2(cif_est)} / MT</span>
  </div>
</div>
""", unsafe_allow_html=True)

    # Métricas resumo
    fob_brl = fob_est * float(cambio)
    cif_brl = cif_est * float(cambio)
    st.markdown(f"""
<div class="met-grid">
  <div class="met-box">
    <div class="met-lbl">FOB Estimado</div>
    <div class="met-val">USD {_fmt2(fob_est)}/MT</div>
    <div class="met-sub">R$ {_fmt2(fob_brl, 0)}/MT</div>
  </div>
  <div class="met-box cif-hero">
    <div class="met-lbl">CIF {destino}</div>
    <div class="met-val">USD {_fmt2(cif_est)}/MT</div>
    <div class="met-sub">R$ {_fmt2(cif_brl, 0)}/MT</div>
  </div>
  <div class="met-box">
    <div class="met-lbl">Volume</div>
    <div class="met-val">{_fmt(volume)} MT</div>
    <div class="met-sub">Porto: {porto_api}</div>
  </div>
  <div class="met-box">
    <div class="met-lbl">Valor Total</div>
    <div class="met-val">USD {_fmt(val_tot)}</div>
    <div class="met-sub">R$ {_fmt(val_tot * float(cambio))}</div>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Comissão — USD/MT fixo ─────────────────────────────────────────────────
    st.markdown('<hr style="border:none;border-top:1px solid #F0F0F0;margin:20px 0 14px">', unsafe_allow_html=True)
    _section("COMISSÃO — GRÃOS EXPORT", "💰")
    comm_usd_mt   = 5.00
    comm_buy_mt   = 2.50
    comm_sell_mt  = 2.50
    comm_total    = comm_usd_mt  * float(volume)
    comm_buy_tot  = comm_buy_mt  * float(volume)
    comm_sell_tot = comm_sell_mt * float(volume)
    st.markdown(f"""
<div class="comm-box">
  <div class="comm-row"><span class="cl">Compra · USD 2,50/MT</span><span class="cv">$ {_fmt(comm_buy_tot)}</span></div>
  <div class="comm-row"><span class="cl">Venda · USD 2,50/MT</span><span class="cv">$ {_fmt(comm_sell_tot)}</span></div>
  <div class="comm-row hi"><span class="cl">Total · USD 5,00/MT</span><span class="cv">$ {_fmt(comm_total)}</span></div>
</div>
""", unsafe_allow_html=True)

    # ── Simulação Avançada SAMBA ───────────────────────────────────────────────
    st.markdown('<hr style="border:none;border-top:1px solid #F0F0F0;margin:20px 0 14px">', unsafe_allow_html=True)
    _section("SIMULAÇÃO AVANÇADA — SAMBA ENGINE", "🚀")
    st.markdown(
        '<div style="font-size:11px;color:#7F7F7F;margin-bottom:12px;'
        'font-family:Montserrat,sans-serif">'
        'Aciona o pricing_builder_service + multimodal_router + ship_service + tax_engine '
        'do SAMBA_LIMPO. Requer SAMBA Quote API em execução.</div>',
        unsafe_allow_html=True,
    )

    if st.button("Executar Simulação Completa SAMBA", key="cot_samba_sim",
                 use_container_width=True):
        with st.spinner("Consultando SAMBA Engine…"):
            sim = _call_samba_quote(
                produto_code=prod_api, volume=float(volume),
                incoterm="CIF", pais_destino=pais_api, porto_saida=porto_api,
                comm_usd_mt=comm_usd_mt, tipo_contrato="SPOT", meses=1,
            )
        if sim:
            st.session_state["graos_samba_sim"] = sim
            st.session_state["graos_samba_cif"] = (
                (sim.get("campiao") or {}).get("cif_mercado_usd_mt", cif_est)
            )
        else:
            st.warning(
                f"SAMBA Quote API não respondeu em {_QUOTE_API}. "
                "Usando estimativa simplificada."
            )

    if st.session_state.get("graos_samba_sim"):
        _render_samba_stack(st.session_state["graos_samba_sim"])
        cif_final = st.session_state.get("graos_samba_cif", cif_est)
        val_tot   = cif_final * float(volume)
    else:
        cif_final = cif_est

    st.markdown(
        f'<p class="cot-note">⚠ {cfg["nota"]} · '
        f'Basis via SAMBA Quote API (fallback operacional se offline). '
        f'Frete marítimo: referência Panamax/Supramax {_DESTINOS.get(destino, {}).get("grupo", "")}. '
        f'Câmbio sujeito a variação. Comissão: USD 2,50/MT compra + USD 2,50/MT venda.</p>',
        unsafe_allow_html=True,
    )

    # ── Cotação formal ─────────────────────────────────────────────────────────
    products_snap = [{
        "name":         commodity,
        "price":        round(cif_final, 2),
        "volume_mt":    float(volume),
        "cbot_usd_mt":  round(cbot_inp, 2),
        "basis_usd_mt": round(basis_inp, 2),
        "fob_usd_mt":   round(fob_est, 2),
        "frete_mar":    round(frete_inp, 2),
        "cif_usd_mt":   round(cif_final, 2),
        "porto_saida":  porto_api,
        "porto_dest":   pais_api,
    }]
    extra = {
        "commodity":   commodity,
        "destino":     destino,
        "rota":        rota,
        "porto_ref":   porto_ref,
        "porto_saida": porto_api,
        "cambio":      float(cambio),
        "fob_usd_mt":  round(fob_est, 2),
        "cif_usd_mt":  round(cif_final, 2),
        "comm_usd_mt": comm_usd_mt,
        "ic45_premium": ic45_premium if commodity == "Açúcar IC45" else 0.0,
    }
    _render_formal_quote_form(
        category=f"GRÃOS — {commodity.upper()} {'ARCO NORTE' if 'Arco Norte' in rota else ''}",
        products_snapshot=products_snap,
        fcl_weight=0.0, fcl_month=0, months=1,
        tot_mt=float(volume), tot_month=val_tot, tot_total=val_tot,
        comm_rate=comm_usd_mt, comm_type="USD_TON",
        extra=extra,
    )


# ─── Helpers para o formulário de cotação formal ──────────────────────────────

def _fmt_br(n: float, decimals: int = 2) -> str:
    """Formata número no padrão brasileiro: 1.234,56 / 1.234"""
    return f"{n:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _first_name_co(full_name: str, max_chars: int = 15) -> str:
    """Primeira palavra do nome da empresa, máx `max_chars` caracteres, uppercase."""
    first = (full_name.strip().split()[0] if full_name.strip() else "BUYER")
    return first[:max_chars].upper()


def _today_parts() -> tuple[str, str, str]:
    """Retorna (DD/MM/YYYY, MM, YYYY) da data atual."""
    d = date.today()
    return d.strftime("%d/%m/%Y"), d.strftime("%m"), str(d.year)


# ─── Formulário de cotação formal ─────────────────────────────────────────────

def _render_formal_quote_form(
    category: str,
    products_snapshot: list[dict],
    fcl_weight: float,
    fcl_month: int,
    months: int,
    tot_mt: float,
    tot_month: float,
    tot_total: float,
    comm_rate: float,
    comm_type: str = "PERCENT",
    extra: dict | None = None,
) -> None:
    """
    Renderiza o formulário 'GERAR COTAÇÃO FORMAL' e dispara a geração do PDF.

    Fluxo:
      • comm_type == "USD_TON"  (Grãos)   → injeta no template real .docx
                                              via process_price_indication()
      • comm_type == "PERCENT" (Proteínas) → gera docx programático
                                              via process_cotacao()
    """
    st.markdown('<hr style="border:none;border-top:1px solid #F0F0F0;margin:16px 0 14px">',
                unsafe_allow_html=True)
    _section("GERAR COTAÇÃO FORMAL", "📄")

    is_graos    = (comm_type == "USD_TON")
    _result_key = f"_pi_result_{comm_type}"   # persiste resultado no session_state

    # ── Form (evita rerun a cada digitação) ────────────────────────────────────
    _dest_prefill = (extra or {}).get("destino", "China")
    _dest_safe    = re.sub(r"[^a-zA-Z0-9_]", "_", _dest_prefill)

    with st.form(key=f"cot_form_{comm_type}_{_dest_safe}", clear_on_submit=False):
        # ── Campos do comprador ────────────────────────────────────────────────
        fb1, fb2 = st.columns(2)
        buyer_name    = fb1.text_input(
            "Empresa receptora", key=f"cot_buyer_name_{comm_type}",
            placeholder="Ex.: Guangdong Foods Co., Ltd."
        )
        buyer_contact = fb2.text_input(
            "Contato / Atenção", key=f"cot_buyer_contact_{comm_type}",
            placeholder="Ex.: Mr. Zhang Wei"
        )

        fc1, fc2, fc3 = st.columns(3)
        buyer_country = fc1.text_input(
            "País destino",
            value=_dest_prefill,
            key=f"cot_buyer_country_{comm_type}_{_dest_safe}",
            placeholder="China",
        )
        validity_days = fc2.number_input(
            "Validade (dias)", 1, 90, 3,
            key=f"cot_validity_{comm_type}",
            help="Padrão Price Indication: 3 business days"
        )
        incoterm = fc3.selectbox(
            "Incoterm", ["CIF", "FOB", "CFR", "DAP"],
            key=f"cot_incoterm_{comm_type}"
        )

        # ── Botão submit (só dispara rerun ao clicar) ──────────────────────────
        _submitted = st.form_submit_button(
            "📄  Gerar Price Indication (PDF)" if is_graos else "📄  Gerar PDF de Cotação Formal",
            use_container_width=True,
        )

    if _submitted:

        # Validação UX — não usa return para não sumir o warning num rerun
        if not buyer_name.strip():
            st.warning("⚠ Preencha o campo **Empresa receptora** antes de gerar.")
        else:
            country_val = (buyer_country.strip() or (extra or {}).get("destino", "China")).upper()

            with st.spinner("📄 Confeccionando Price Indication na nuvem…"):
                try:
                    if is_graos:
                        # ── GRÃOS: injeta no template real .docx ──────────────────
                        # Reload garante que hot-reload do Streamlit não use pyc stale
                        try:
                            from agents.cotacao_agent import process_price_indication
                        except ImportError:
                            import importlib, agents.cotacao_agent as _ca
                            importlib.reload(_ca)
                            process_price_indication = _ca.process_price_indication

                        p0           = products_snapshot[0] if products_snapshot else {}
                        dd, mm, yyyy = _today_parts()
                        ex           = extra or {}
                        porto_saida  = ex.get("porto_saida", "PARANAGUA")

                        _porto_label = {
                            "PARANAGUA":   "Paranaguá",
                            "BARCARENA":   "Barcarena",
                            "SANTOS":      "Santos",
                            "SALINOPOLIS": "Salinópolis",
                            "ITACOATIARA": "Itacoatiara",
                        }
                        porto_display = _porto_label.get(porto_saida, porto_saida.title())

                        payload_cotacao = {
                            "document_type":  "PRICE_INDICATION",
                            "template_name":  "2_PRICE_PREQUOTATION_SOY_SUGAR_CORN.docx",
                            "output_format":  "pdf",
                            "dynamic_fields": {
                                "COMODITIE_TYPE":     ex.get("commodity", category).upper(),
                                # Template V3: CIF {CITY}, {COUNTRY} — ex.: "CIF Main Port, CHINA"
                                "CITY":               "Main Port",
                                "COUNTRY":            country_val,
                                "DD/MM/YYYY":         dd,
                                "MM":                 mm,
                                "YYYY":               yyyy,
                                "FIRST NAME Company": _first_name_co(buyer_name),
                                "FULL NAME Company":  buyer_name.strip().upper(),
                                "PORTO":              porto_display,
                            },
                            "financial_fields": {
                                # Chaves limpas → _pi_build_subs mapeia para nomes exatos do XML
                                "PRICE BASIS":            _fmt_br(p0.get("cbot_usd_mt",  0.0)),
                                "BASIS REFERENCIA PORTO": _fmt_br(p0.get("basis_usd_mt", 0.0)),
                                "PRICE FOB":              _fmt_br(p0.get("fob_usd_mt",   0.0)),
                                "PRICE FREIGHT":          _fmt_br(p0.get("frete_mar",    0.0)),
                                "FINAL_PRICE":            _fmt_br(p0.get("cif_usd_mt",   0.0)),
                                # Comissão TOTAL por embarque: volume × USD 5,00/MT
                                "COMISSION_CONTRACT":     _fmt_br(tot_mt * 5.0),
                                "QUANTITY_MT":            _fmt_br(tot_mt, 0),
                            },
                        }
                        result = process_price_indication(payload_cotacao)

                    else:
                        # ── PROTEÍNAS: gera docx programático ────────────────────
                        try:
                            from agents.cotacao_agent import process_cotacao
                        except ImportError:
                            import importlib, agents.cotacao_agent as _ca
                            importlib.reload(_ca)
                            process_cotacao = _ca.process_cotacao

                        payload_cotacao = {
                            "document_type":  "PRICE_INDICATION",
                            "category":       category,
                            "products":       products_snapshot,
                            "fcl_weight":     fcl_weight,
                            "fcl_month":      fcl_month,
                            "months":         months,
                            "tot_mt":         tot_mt,
                            "tot_month":      tot_month,
                            "tot_total":      tot_total,
                            "comm_rate":      comm_rate,
                            "comm_type":      comm_type,
                            "buyer": {
                                "name":    buyer_name.strip(),
                                "contact": buyer_contact.strip(),
                                "country": country_val,
                            },
                            "validity_days": int(validity_days),
                            "incoterm":      incoterm,
                            "extra":         extra or {},
                            "date":          date.today().isoformat(),
                        }
                        result = process_cotacao(payload_cotacao)

                    # ── Persiste resultado no session_state → exibido fora do bloco
                    st.session_state[_result_key] = result

                except Exception as exc:
                    import traceback as _tb
                    st.session_state[_result_key] = {
                        "error":      str(exc),
                        "_traceback": _tb.format_exc(),
                    }

    # ── Resultado persistente — exibido fora do bloco do botão ────────────────
    # Permanece visível entre reruns; limpo quando novo PDF é gerado
    if _result_key in st.session_state:
        _show_result(st.session_state[_result_key], key_suffix=comm_type)


def _show_result(result: dict, key_suffix: str = "") -> None:
    """Exibe resultado de geração de PDF.  key_suffix garante chave única por tipo."""
    if result.get("error"):
        st.error(f"❌ Erro ao gerar documento: {result['error']}")
        if result.get("_traceback"):
            with st.expander("🔍 Detalhes técnicos — clique para expandir"):
                st.code(result["_traceback"], language="python")
        return
    link        = result.get("web_link", "")
    filename    = result.get("filename", "cotacao.pdf")
    file_bytes  = result.get("file_bytes")
    pdf_failed  = result.get("pdf_failed", False)

    # Detecta tipo real pelo nome do arquivo
    is_docx = filename.lower().endswith(".docx")
    mime    = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        if is_docx else "application/pdf"
    )
    dl_label = "⬇  Baixar Word (.docx)" if is_docx else "⬇  Baixar PDF"

    link_html  = (f'<a href="{link}" target="_blank" style="color:#FA8200;font-weight:700;'
                  f'text-decoration:none">📂 Abrir no Google Drive</a>') if link else ""
    st.markdown(f"""
<div class="cot-result">
  <div style="font-size:9px;letter-spacing:2px;font-weight:700;color:#7F7F7F;margin-bottom:7px;
  font-family:Montserrat,sans-serif;text-transform:uppercase">✅ DOCUMENTO GERADO</div>
  <div style="font-size:14px;font-weight:600;color:#262626;margin-bottom:8px;
  font-family:Montserrat,sans-serif">{filename}</div>
  {link_html}
</div>
""", unsafe_allow_html=True)
    if pdf_failed:
        st.warning(
            "⚠️ Conversão PDF indisponível — o arquivo foi gerado em formato Word (.docx). "
            "Abra no Word e exporte como PDF se necessário.",
            icon=None,
        )
    if file_bytes:
        st.download_button(
            dl_label,
            data=file_bytes,
            file_name=filename,
            mime=mime,
            key=f"cot_dl_pdf_{key_suffix or 'default'}",
        )


# ─── Entry point ──────────────────────────────────────────────────────────────

def render_cotacao_tab() -> None:
    """Chamado dentro de `with abas[1]:` no streamlit_app.py."""
    st.markdown(_CSS, unsafe_allow_html=True)

    cat = st.radio(
        "Categoria",
        ["FRANGO", "SUÍNO", "GRÃOS"],
        horizontal=True,
        key="cot_cat",
        label_visibility="collapsed",
    )

    st.markdown("<div style='margin-top:4px'></div>", unsafe_allow_html=True)

    if cat == "FRANGO":
        _render_frango()
    elif cat == "SUÍNO":
        _render_suina()
    else:
        _render_graos()
