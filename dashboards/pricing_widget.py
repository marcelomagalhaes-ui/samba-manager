"""
dashboards/pricing_widget.py
Consulta de preço EXW / FOB / CIF — resposta imediata ao trader.
"Quanto está a soja FOB Outeiro hoje?"
Stack: EXW (origem regional) → FOB (porto BR) → CIF (destino)
"""
import math
import sys
from pathlib import Path
from datetime import datetime
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import get_engine
import sqlalchemy

# ─────────────────────────────────────────────────────────────────────────────
# PRODUTOS — SOY / CORN / SUGAR (VHP · IC 45 · IC 150)
# ─────────────────────────────────────────────────────────────────────────────
_PRODUTOS = {
    "SOY": {
        "label": "Soja", "cbot_key": "cbot_soy_usd_mt",
        "kg_saca": 60, "basis_key": "basis_soy",
        "icumsa": None, "sugar_premium": 0.0,
    },
    "CORN": {
        "label": "Milho", "cbot_key": "cbot_corn_usd_mt",
        "kg_saca": 60, "basis_key": "basis_corn",
        "icumsa": None, "sugar_premium": 0.0,
    },
    "SUGAR_VHP": {
        "label": "Açúcar VHP", "cbot_key": "ice_sugar_usd_mt",
        "kg_saca": 50, "basis_key": "basis_sugar",
        "icumsa": "800–1800", "sugar_premium": 0.0,
        "descricao": "Very High Polarization — contrato ICE #11 base",
    },
    "SUGAR_IC150": {
        "label": "Açúcar IC 150", "cbot_key": "ice_sugar_usd_mt",
        "kg_saca": 50, "basis_key": "basis_sugar",
        "icumsa": "150", "sugar_premium": 4.0,
        "descricao": "ICUMSA 150 — açúcar cristal branco, premium sobre VHP",
    },
    "SUGAR_IC45": {
        "label": "Açúcar IC 45", "cbot_key": "ice_sugar_usd_mt",
        "kg_saca": 50, "basis_key": "basis_sugar",
        "icumsa": "45", "sugar_premium": 9.0,
        "descricao": "ICUMSA 45 — açúcar refinado branco, maior premium sobre VHP",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# PORTOS BRASILEIROS — elevação real (tb_elevacao_portuaria_br.csv)
# ─────────────────────────────────────────────────────────────────────────────
_PORTOS = {
    # ── ARCO NORTE ─────────────────────────────────────────────────────────────
    # Basis soy/corn: historico_basis_portos_corn_soy.csv (mai/2026)
    # Elev: tb_elevacao_portuaria_br.csv
    # Basis sugar: historico_basis_portos_acucar.csv (valores NE estimados)
    "Outeiro (Belém/PA)": {
        "code": "BELEM",    "uf": "PA", "regiao": "Arco Norte",
        "basis_soy": 13.3,  "basis_corn": 17.1,  "basis_sugar": 44.0,
        "elev_usd": 14.0,   "sugar_porto": False,
        # basis ≈ BARCARENA + 0.0 (portos próximos, mesmo hub)
    },
    "Barcarena (PA)": {
        "code": "BARCARENA","uf": "PA", "regiao": "Arco Norte",
        "basis_soy": 13.3,  "basis_corn": 17.0,  "basis_sugar": 44.0,
        "elev_usd": 15.0,   "sugar_porto": False,
        # fonte CSV: SOY=13.28 CORN=16.98
    },
    "Santarém (PA)": {
        "code": "SANTAREM", "uf": "PA", "regiao": "Arco Norte",
        "basis_soy": 12.5,  "basis_corn": 16.0,  "basis_sugar": 43.0,
        "elev_usd": 14.5,   "sugar_porto": False,
        # estimado ≈ BARCARENA − 0.8 (rio acima, custo interno adicional)
    },
    "Miritituba (PA)": {
        "code": "MIRITITUBA","uf": "PA","regiao": "Arco Norte",
        "basis_soy": 11.5,  "basis_corn": 15.0,  "basis_sugar": 42.0,
        "elev_usd": 5.0,    "sugar_porto": False,
        "obs": "Transbordo hidroviário — elev reduzida",
    },
    "Itacoatiara (AM)": {
        "code": "ITACOATIARA","uf": "AM","regiao": "Arco Norte",
        "basis_soy": 12.0,  "basis_corn": 15.5,  "basis_sugar": 42.0,
        "elev_usd": 10.0,   "sugar_porto": False,
        # estimado ≈ BARCARENA − 1.3 (frete hidroviário adicional)
    },
    "Itaqui (MA)": {
        "code": "ITAQUI",   "uf": "MA", "regiao": "Nordeste",
        "basis_soy": 14.0,  "basis_corn": 17.7,  "basis_sugar": 44.0,
        "elev_usd": 11.0,   "sugar_porto": False,
        # fonte CSV: SOY=13.98 CORN=17.68
    },
    # ── SUL / SUDESTE ───────────────────────────────────────────────────────────
    "Paranaguá (PR)": {
        "code": "PARANAGUA","uf": "PR", "regiao": "Sul",
        "basis_soy": 11.1,  "basis_corn": 14.8,  "basis_sugar": 38.9,
        "elev_usd": 13.0,   "sugar_porto": True,
        # fonte CSV: SOY=11.08 CORN=14.78 SUGAR_VHP=38.86
    },
    "Santos (SP)": {
        "code": "SANTOS",   "uf": "SP", "regiao": "Sudeste",
        "basis_soy": 12.9,  "basis_corn": 16.6,  "basis_sugar": 40.9,
        "elev_usd": 14.0,   "sugar_porto": True,
        # fonte CSV: SOY=12.88 CORN=16.58 SUGAR_VHP=40.86
    },
    "São Francisco (SC)": {
        "code": "SAO_FRANCISCO_SUL","uf": "SC","regiao": "Sul",
        "basis_soy": 10.5,  "basis_corn": 14.1,  "basis_sugar": 37.0,
        "elev_usd": 12.5,   "sugar_porto": False,
        # estimado ≈ PARANAGUA − 0.6 (menor terminal)
    },
    "Rio Grande (RS)": {
        "code": "RIO_GRANDE","uf": "RS", "regiao": "Sul",
        "basis_soy": 10.4,  "basis_corn": 14.1,  "basis_sugar": 37.0,
        "elev_usd": 13.5,   "sugar_porto": False,
        # fonte CSV: SOY=10.38 CORN=14.08
    },
    "Vitória (ES)": {
        "code": "VITORIA",  "uf": "ES", "regiao": "Sudeste",
        "basis_soy": 12.0,  "basis_corn": 15.5,  "basis_sugar": 38.0,
        "elev_usd": 12.0,   "sugar_porto": False,
        # estimado ≈ SANTOS − 0.9 (menor liquidez)
    },
    # ── NORDESTE / AÇÚCAR ──────────────────────────────────────────────────────
    "Maceió (AL)": {
        "code": "MACEIO",   "uf": "AL", "regiao": "Nordeste",
        "basis_soy": 13.5,  "basis_corn": 17.5,  "basis_sugar": 45.9,
        "elev_usd": 12.0,   "sugar_porto": True,
        "obs": "Principal porto açúcar VHP/IC45",
        # fonte CSV sugar: SUGAR_VHP=45.86
    },
    "Recife (PE)": {
        "code": "RECIFE",   "uf": "PE", "regiao": "Nordeste",
        "basis_soy": 13.5,  "basis_corn": 17.5,  "basis_sugar": 45.9,
        "elev_usd": 12.0,   "sugar_porto": True,
        # fonte CSV sugar: SUGAR_VHP=45.86
    },
    "Salvador (BA)": {
        "code": "SALVADOR", "uf": "BA", "regiao": "Nordeste",
        "basis_soy": 13.0,  "basis_corn": 17.0,  "basis_sugar": 42.0,
        "elev_usd": 14.0,   "sugar_porto": False,
        # estimado ≈ ITAQUI − 1.0
    },
    "Pecém (CE)": {
        "code": "PECEM",    "uf": "CE", "regiao": "Nordeste",
        "basis_soy": 14.0,  "basis_corn": 17.7,  "basis_sugar": 44.0,
        "elev_usd": 12.0,   "sugar_porto": False,
        # estimado ≈ ITAQUI (hub NE similar)
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# PORTO DWT MÁXIMO — tb_portos_br.csv
# ─────────────────────────────────────────────────────────────────────────────
_PORTO_MAX_DWT = {
    "SANTOS":            80_000,
    "PARANAGUA":         75_000,
    "ITAQUI":           200_000,
    "BARCARENA":         70_000,
    "BELEM":             35_000,   # Porto Outeiro — calado limitado
    "SANTAREM":          65_000,
    "MIRITITUBA":        35_000,   # barges / Handysize apenas
    "ITACOATIARA":       45_000,
    "SAO_FRANCISCO_SUL": 70_000,
    "RIO_GRANDE":        80_000,
    "VITORIA":           65_000,
    "MACEIO":            45_000,   # calado limitado
    "RECIFE":            35_000,   # Sugar Terminal calado ~10m
    "SALVADOR":          60_000,
    "PECEM":            180_000,
}

# ─────────────────────────────────────────────────────────────────────────────
# DISTÂNCIAS MARÍTIMAS — fonte: tb_core_global_ports_agro_modelA_v1.csv
# Rota ótima: Suez (Ásia/OM/África/Europa), Panama (Américas), direto (vizinhos)
# 28 destinos × 7 portos base — dados reais do sistema anterior Samba Limpo
# Arco Norte (Barcarena/Itaqui/Outeiro) com distâncias precisas do CSV.
# ─────────────────────────────────────────────────────────────────────────────
_DIST_BASE = {
    # porto_csv_code → {destino_zone → dist_nm}
    "SANTOS": {
        # ÁSIA (via Suez)
        "China_N": 11000, "China_S": 10850, "Vietnam": 10900, "Indonesia": 11100,
        "India_W":  8500, "India_E":  9000,  "Korea": 11100, "Japan":   11350,
        "Malaysia": 10250,"Thailand": 10850, "Singapore": 10000,"SriLanka":8800,
        # ORIENTE MÉDIO
        "Jeddah":   6500, "Dubai":    7200,
        # ÁFRICA
        "Egypt":    5400, "Morocco":  4100,  "Nigeria":  3900, "Ghana":   3700, "SAfrika": 4600,
        # EUROPA
        "Rotterdam":5600, "Valencia": 4900,  "Piraeus":  6000,
        # AMÉRICAS (via Panama)
        "USA_Gulf": 3800, "USA_East": 4500,  "Canada":   6200, "Mexico":  3600, "Argentina": 1200, "Chile": 5100,
    },
    "PARANAGUA": {
        "China_N": 11150, "China_S": 11000, "Vietnam": 11050, "Indonesia": 11250,
        "India_W":  8650, "India_E":  9150, "Korea":   11250, "Japan":   11500,
        "Malaysia": 10400,"Thailand": 11000, "Singapore": 10150,"SriLanka":8950,
        "Jeddah":   6650, "Dubai":    7350,
        "Egypt":    5550, "Morocco":  4250, "Nigeria":  4050, "Ghana":   3850, "SAfrika": 4750,
        "Rotterdam":5750, "Valencia": 5050, "Piraeus":  6150,
        "USA_Gulf": 3950, "USA_East": 4650, "Canada":   6350, "Mexico":  3750, "Argentina": 1350, "Chile": 5250,
    },
    "RIO_GRANDE": {
        "China_N": 11350, "China_S": 11150, "Vietnam": 11250, "Indonesia": 11400,
        "India_W":  9000, "India_E":  9500, "Korea":   11400, "Japan":   11650,
        "Malaysia": 10650,"Thailand": 11150, "Singapore": 10350,"SriLanka":9300,
        "Jeddah":   6800, "Dubai":    7500,
        "Egypt":    5700, "Morocco":  4400, "Nigeria":  4200, "Ghana":   4000, "SAfrika": 4900,
        "Rotterdam":5900, "Valencia": 5200, "Piraeus":  6300,
        "USA_Gulf": 4100, "USA_East": 4800, "Canada":   6500, "Mexico":  3900, "Argentina": 1500, "Chile": 5400,
    },
    "VITORIA": {
        "China_N": 10600, "China_S": 10400, "Vietnam": 10450, "Indonesia": 10700,
        "India_W":  8200, "India_E":  8700, "Korea":   10700, "Japan":   10900,
        "Malaysia":  9850,"Thailand": 10400, "Singapore": 9600, "SriLanka":8500,
        "Jeddah":   6200, "Dubai":    6900,
        "Egypt":    5100, "Morocco":  3800, "Nigeria":  3600, "Ghana":   3400, "SAfrika": 4300,
        "Rotterdam":5300, "Valencia": 4600, "Piraeus":  5700,
        "USA_Gulf": 3500, "USA_East": 4200, "Canada":   5900, "Mexico":  3300, "Argentina": 1100, "Chile": 4800,
    },
    "ITAQUI": {
        # ÁSIA (via Suez) — ~200 NM mais perto que Santos
        "China_N": 10350, "China_S": 10200, "Vietnam": 10250, "Indonesia": 10450,
        "India_W": 7800,  "India_E": 8300,  "Korea": 10450, "Japan": 10700,
        "Malaysia": 9600, "Thailand": 10200,"Singapore": 9350,"SriLanka": 8100,
        # ORIENTE MÉDIO
        "Jeddah": 5900,   "Dubai": 6600,
        # ÁFRICA
        "Egypt":  4800,   "Morocco": 3500, "Nigeria": 3200, "Ghana": 3000, "SAfrika": 4000,
        # EUROPA
        "Rotterdam": 5000,"Valencia": 4300,"Piraeus": 5400,
        # AMÉRICAS
        "USA_Gulf": 3200, "USA_East": 4000,"Canada": 5600, "Mexico": 3000, "Argentina": 2400, "Chile": 4500,
    },
    "BARCARENA": {
        # ÁSIA — dados reais do CSV (tb_core_global_ports_agro_modelA_v1)
        "China_N": 10400, "China_S": 10250, "Vietnam": 10300, "Indonesia": 10500,
        "India_W": 7850,  "India_E": 8350,  "Korea": 10500, "Japan": 10750,
        "Malaysia": 9650, "Thailand": 10250,"Singapore": 9400,"SriLanka": 8150,
        # ORIENTE MÉDIO
        "Jeddah": 5950,   "Dubai": 6650,
        # ÁFRICA
        "Egypt":  4850,   "Morocco": 3550, "Nigeria": 3250, "Ghana": 3050, "SAfrika": 4050,
        # EUROPA
        "Rotterdam": 5050,"Valencia": 4350,"Piraeus": 5450,
        # AMÉRICAS (via Panama — dados reais CSV)
        "USA_Gulf": 3100, "USA_East": 3900,"Canada": 5500, "Mexico": 2900, "Argentina": 2300, "Chile": 4400,
    },
    "PECEM": {
        # ÁSIA
        "China_N": 10000, "China_S":  9850, "Vietnam":  9700, "Indonesia":  9950,
        "India_W": 7300,  "India_E":  8050,  "Korea": 10150, "Japan": 10400,
        "Malaysia": 9300, "Thailand":  9900,"Singapore": 9050,"SriLanka": 7850,
        # ORIENTE MÉDIO
        "Jeddah": 5550,   "Dubai": 6250,
        # ÁFRICA
        "Egypt":  4450,   "Morocco": 3150, "Nigeria": 2950, "Ghana": 2750, "SAfrika": 3650,
        # EUROPA
        "Rotterdam": 4650,"Valencia": 3950,"Piraeus": 5050,
        # AMÉRICAS
        "USA_Gulf": 2800, "USA_East": 3500,"Canada": 5100, "Mexico": 2600, "Argentina": 2750, "Chile": 4100,
    },
}

# Mapping: porto_code → (csv_base_code, nm_offset)
# Arco Norte river ports transship via Barcarena — ocean distance same
_PORTO_DIST_MAP = {
    "BELEM":             ("BARCARENA",  +30),
    "BARCARENA":         ("BARCARENA",    0),
    "SANTAREM":          ("BARCARENA",    0),
    "MIRITITUBA":        ("BARCARENA",    0),
    "ITACOATIARA":       ("BARCARENA",    0),
    "ITAQUI":            ("ITAQUI",       0),
    "PARANAGUA":         ("PARANAGUA",    0),
    "SANTOS":            ("SANTOS",       0),
    "SAO_FRANCISCO_SUL": ("PARANAGUA", +130),
    "RIO_GRANDE":        ("RIO_GRANDE",   0),
    "VITORIA":           ("VITORIA",      0),
    "MACEIO":            ("PECEM",     +300),
    "RECIFE":            ("PECEM",     +200),
    "SALVADOR":          ("VITORIA",   -400),
    "PECEM":             ("PECEM",       0),
}

# Destino CIF label → chave interna do _DIST_BASE
# 28 destinos com dados reais do CSV Samba Limpo (tb_core_global_ports_agro_modelA_v1)
_DEST_KEYS = {
    # ── ÁSIA ──────────────────────────────────────────────────────
    "China — Shanghai / Norte":     "China_N",
    "China — Guangzhou / Sul":      "China_S",
    "Vietnã — Ho Chi Minh":         "Vietnam",
    "Indonésia — Jakarta":          "Indonesia",
    "Índia — Mundra / Kandla":      "India_W",
    "Índia — Chennai / Leste":      "India_E",
    "Coreia do Sul — Busan":        "Korea",
    "Japão — Yokohama":             "Japan",
    "Malásia — Port Klang":         "Malaysia",
    "Tailândia — Laem Chabang":     "Thailand",
    "Singapura":                    "Singapore",
    "Sri Lanka — Colombo":          "SriLanka",
    # ── ORIENTE MÉDIO ─────────────────────────────────────────────
    "Arábia Saudita — Jeddah":      "Jeddah",
    "Golfo Pérsico — Dubai / EAU":  "Dubai",
    # ── ÁFRICA ────────────────────────────────────────────────────
    "Egito — Alexandria":           "Egypt",
    "Marrocos — Tânger":            "Morocco",
    "Nigéria — Lagos":              "Nigeria",
    "Gana — Tema":                  "Ghana",
    "África do Sul — Durban":       "SAfrika",
    # ── EUROPA ────────────────────────────────────────────────────
    "Europa NW — Rotterdam":        "Rotterdam",
    "Mediterrâneo — Valencia":      "Valencia",
    "Grécia — Pireu":               "Piraeus",
    # ── AMÉRICAS ──────────────────────────────────────────────────
    "EUA — Golfo (New Orleans)":    "USA_Gulf",
    "EUA — Costa Leste (Norfolk)":  "USA_East",
    "Canadá — Vancouver":           "Canada",
    "México — Veracruz":            "Mexico",
    "Argentina — Rosário":          "Argentina",
    "Chile — San Antonio":          "Chile",
}

_DESTINOS_CIF = list(_DEST_KEYS.keys())

# ── Agrupamento regional para UI do seletor CIF ──────────────────────────────
_DEST_GRUPOS = {
    "🌏 Ásia":          [
        "China — Shanghai / Norte", "China — Guangzhou / Sul",
        "Vietnã — Ho Chi Minh", "Indonésia — Jakarta",
        "Índia — Mundra / Kandla", "Índia — Chennai / Leste",
        "Coreia do Sul — Busan", "Japão — Yokohama",
        "Malásia — Port Klang", "Tailândia — Laem Chabang",
        "Singapura", "Sri Lanka — Colombo",
    ],
    "🕌 Oriente Médio": [
        "Arábia Saudita — Jeddah", "Golfo Pérsico — Dubai / EAU",
    ],
    "🌍 África": [
        "Egito — Alexandria", "Marrocos — Tânger",
        "Nigéria — Lagos", "Gana — Tema", "África do Sul — Durban",
    ],
    "🇪🇺 Europa": [
        "Europa NW — Rotterdam", "Mediterrâneo — Valencia", "Grécia — Pireu",
    ],
    "🌎 Américas": [
        "EUA — Golfo (New Orleans)", "EUA — Costa Leste (Norfolk)",
        "Canadá — Vancouver", "México — Veracruz",
        "Argentina — Rosário", "Chile — San Antonio",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# TAXAS DE CARREGAMENTO (t/dia) — tb_handbook_ports.csv
# ─────────────────────────────────────────────────────────────────────────────
_LOAD_RATES = {
    "SANTOS":            36_000,   # TGG / COFCO / Cooper-Sugar 36k t/d cada
    "PARANAGUA":         24_000,   # Soceppar/Bunge / Pasa
    "RIO_GRANDE":        28_000,   # Bunge ~1200 t/h ≈ 28.8k t/d
    "VITORIA":           12_000,
    "ITAQUI":            25_000,   # berths 100 / 103 / 105
    "BARCARENA":         20_000,
    "BELEM":             15_000,
    "SANTAREM":          18_000,
    "MIRITITUBA":        10_000,
    "ITACOATIARA":       10_000,
    "SAO_FRANCISCO_SUL": 20_000,
    "MACEIO":            12_000,   # Sugar Terminal 500 t/h = 12k t/d
    "RECIFE":            10_000,   # Sugar Terminal
    "SALVADOR":          18_000,
    "PECEM":             15_000,
}

# Taxas de descarga padrão por destino (t/dia)
_DISCH_RATES = {
    "China_N":    20_000,  "China_S":    20_000,
    "Vietnam":    15_000,  "Indonesia":  15_000,
    "India_W":    18_000,  "India_E":    16_000,
    "Korea":      20_000,  "Japan":      20_000,
    "Malaysia":   15_000,  "Thailand":   15_000,
    "Singapore":  18_000,  "SriLanka":   12_000,
    "Jeddah":     15_000,  "Dubai":      15_000,
    "Egypt":      15_000,  "Morocco":    12_000,
    "Nigeria":    10_000,  "Ghana":      10_000,  "SAfrika":  18_000,
    "Rotterdam":  25_000,  "Valencia":   18_000,  "Piraeus":  15_000,
    "USA_Gulf":   25_000,  "USA_East":   22_000,
    "Canada":     20_000,  "Mexico":     15_000,
    "Argentina":  20_000,  "Chile":      15_000,
}

# PDA (Port Disbursement Account) em USD/MT de carga
_PDA_LOAD = {   # origem Brasil
    "SANTOS":            0.75,
    "PARANAGUA":         0.60,
    "RIO_GRANDE":        0.60,
    "VITORIA":           0.55,
    "ITAQUI":            0.55,
    "BARCARENA":         0.55,
    "BELEM":             0.50,
    "SANTAREM":          0.50,
    "MIRITITUBA":        0.50,
    "ITACOATIARA":       0.50,
    "SAO_FRANCISCO_SUL": 0.55,
    "MACEIO":            0.55,
    "RECIFE":            0.55,
    "SALVADOR":          0.55,
    "PECEM":             0.55,
}

_PDA_DISCH = {  # destino
    "China_N":   0.70,  "China_S":   0.70,
    "Vietnam":   0.55,  "Indonesia": 0.55,
    "India_W":   0.60,  "India_E":   0.60,
    "Korea":     0.65,  "Japan":     0.65,
    "Malaysia":  0.50,  "Thailand":  0.50,
    "Singapore": 0.55,  "SriLanka":  0.45,
    "Jeddah":    0.45,  "Dubai":     0.50,
    "Egypt":     0.50,  "Morocco":   0.45,
    "Nigeria":   0.40,  "Ghana":     0.40,  "SAfrika":  0.55,
    "Rotterdam": 0.60,  "Valencia":  0.55,  "Piraeus":  0.55,
    "USA_Gulf":  0.65,  "USA_East":  0.65,
    "Canada":    0.60,  "Mexico":    0.50,
    "Argentina": 0.55,  "Chile":     0.50,
}

# ─────────────────────────────────────────────────────────────────────────────
# CLASSES DE NAVIO — consumo real, DWT de carga, hire diário de referência
# Dados: tb_tipos_navios_agro.csv + referências Baltic Dry 2024
# ─────────────────────────────────────────────────────────────────────────────
_NAVIOS = {
    "Handysize (10–35k DWT)": {
        "dwt_min": 10_000, "dwt_max": 35_000, "dwt_cargo": 32_000,
        "consumo_mar": 25, "consumo_porto": 4, "daily_hire": 8_500,
    },
    "Supramax (50–60k DWT)": {
        "dwt_min": 50_000, "dwt_max": 60_000, "dwt_cargo": 55_000,
        "consumo_mar": 30, "consumo_porto": 5, "daily_hire": 10_000,
    },
    "Panamax (65–80k DWT)": {
        "dwt_min": 65_000, "dwt_max": 80_000, "dwt_cargo": 72_000,
        "consumo_mar": 35, "consumo_porto": 5, "daily_hire": 12_000,
    },
    "Kamsarmax (80–85k DWT)": {
        "dwt_min": 80_000, "dwt_max": 85_000, "dwt_cargo": 78_000,
        "consumo_mar": 36, "consumo_porto": 5, "daily_hire": 13_000,
    },
    "Post-Panamax (85–110k DWT)": {
        "dwt_min": 85_000, "dwt_max": 110_000, "dwt_cargo": 100_000,
        "consumo_mar": 42, "consumo_porto": 6, "daily_hire": 15_000,
    },
    "Capesize (150–200k DWT)": {
        "dwt_min": 150_000, "dwt_max": 200_000, "dwt_cargo": 175_000,
        "consumo_mar": 50, "consumo_porto": 6, "daily_hire": 18_000,
    },
}

# Demurrage: crédito CIF — vendedor controla embarque, sem risco para comprador
_DEMURRAGE_CIF_CREDITO_USD = 1.00

# Margem do armador sobre custos operacionais (bunker + hire) — 15-20% mercado
_MARGEM_ARMADOR = 1.15

# Dias de espera em fundeio (anchorage) — origem BR
# Base: off-peak. Safra (mar–ago) aplica fator ×1.5
_CONGESTION_ORIGIN = {
    "SANTOS":            4.0,
    "PARANAGUA":         5.0,
    "RIO_GRANDE":        4.0,
    "VITORIA":           3.0,
    "ITAQUI":            5.0,
    "BARCARENA":         5.0,
    "BELEM":             3.0,
    "SANTAREM":          3.0,
    "MIRITITUBA":        2.0,
    "ITACOATIARA":       2.0,
    "SAO_FRANCISCO_SUL": 4.0,
    "MACEIO":            3.0,
    "RECIFE":            3.0,
    "SALVADOR":          3.0,
    "PECEM":             4.0,
}

# Dias de espera em fundeio — destino (PortCongestionService)
_CONGESTION_DEST = {
    "China_N":   4.0,  "China_S":   3.5,
    "Vietnam":   3.0,  "Indonesia": 3.0,
    "India_W":   2.5,  "India_E":   2.5,
    "Korea":     2.0,  "Japan":     1.5,
    "Malaysia":  2.0,  "Thailand":  2.5,
    "Singapore": 1.5,  "SriLanka":  2.0,
    "Jeddah":    2.0,  "Dubai":     2.5,
    "Egypt":     3.0,  "Morocco":   2.0,
    "Nigeria":   4.0,  "Ghana":     3.5,  "SAfrika":  2.0,
    "Rotterdam": 1.5,  "Valencia":  2.0,  "Piraeus":  2.5,
    "USA_Gulf":  2.0,  "USA_East":  1.5,
    "Canada":    1.5,  "Mexico":    2.0,
    "Argentina": 2.5,  "Chile":     2.0,
    # legado (fallback) ────────────────────────────────────────────
    "China":              4.0,
    "Vietnã":             3.0,
    "Indonésia":          3.0,
    "Índia":              7.0,
    "Oriente Médio":      4.0,
    "Europa NW":          2.0,
    "Egito / N. África":  4.0,
    "África Subsaariana": 9.0,
    "EUA / Golfo":        3.0,
}

# Escala de hire por classe relativa ao Panamax (BDI-proxy) = 1.0
_HIRE_SCALE = {
    "Handysize (10–35k DWT)":     0.55,
    "Supramax (50–60k DWT)":      0.75,
    "Panamax (65–80k DWT)":       1.00,
    "Kamsarmax (80–85k DWT)":     1.08,
    "Post-Panamax (85–110k DWT)": 1.25,
    "Capesize (150–200k DWT)":    1.50,
}


# ─────────────────────────────────────────────────────────────────────────────
# SELEÇÃO DE NAVIO — automática por volume + restrição DWT do porto
# ─────────────────────────────────────────────────────────────────────────────

def _navio_para_porto_volume(volume_mt: float, porto_code: str) -> str:
    """Seleciona menor classe de navio que cobre o volume em 1 viagem, respeitando DWT do porto."""
    max_port_dwt = _PORTO_MAX_DWT.get(porto_code, 80_000)
    # Ordenado por DWT crescente: (nome, dwt_cargo, dwt_max)
    candidatos = [
        ("Handysize (10–35k DWT)",     32_000,  35_000),
        ("Supramax (50–60k DWT)",      55_000,  60_000),
        ("Panamax (65–80k DWT)",       72_000,  80_000),
        ("Kamsarmax (80–85k DWT)",     78_000,  85_000),
        ("Post-Panamax (85–110k DWT)", 100_000, 110_000),
        ("Capesize (150–200k DWT)",    175_000, 200_000),
    ]
    # Filtra pelos elegíveis ao porto
    elegiveis = [(n, c, d) for n, c, d in candidatos if d <= max_port_dwt]
    if not elegiveis:
        elegiveis = [candidatos[0]]
    # Menor navio que cobre volume em 1 viagem
    for nome, cargo, _ in elegiveis:
        if cargo >= volume_mt:
            return nome
    # Volume supera todos os elegíveis → usa o maior disponível
    return elegiveis[-1][0]


# ─────────────────────────────────────────────────────────────────────────────
# MOTOR DE CÁLCULO DE FRETE MARÍTIMO
# Fontes: novas_rotas_maritimas.csv, tb_tipos_navios_agro.csv, tb_handbook_ports.csv
# ─────────────────────────────────────────────────────────────────────────────

def _get_dist_nm(porto_code: str, destino: str) -> int:
    base_code, offset = _PORTO_DIST_MAP.get(porto_code, ("SANTOS", 0))
    dest_key = _DEST_KEYS.get(destino, "China_N")   # China_N como fallback válido
    base = _DIST_BASE.get(base_code, _DIST_BASE["SANTOS"])
    return int(base.get(dest_key, 10_000) + offset)


def _calcular_frete_maritimo(porto_code: str, destino: str, volume: float,
                              navio_key: str, bunker: float,
                              daily_hire_market: int = 0) -> dict:
    """
    Calcula frete marítimo USD/MT a partir de primeiros princípios.
    Inclui: distância × consumo, hire (BDI-dinâmico se disponível),
    congestionamento de porto (safra vs. off-peak), margem do armador e PDA.
    """
    dist_nm = _get_dist_nm(porto_code, destino)
    navio   = _NAVIOS[navio_key]
    dwt_cargo    = navio["dwt_cargo"]
    consumo_mar  = navio["consumo_mar"]
    consumo_port = navio["consumo_porto"]

    # Hire: mercado BDI se disponível, senão estático por classe
    if daily_hire_market > 0:
        daily_hire = int(daily_hire_market * _HIRE_SCALE[navio_key])
        hire_fonte = "BDI"
    else:
        daily_hire = navio["daily_hire"]
        hire_fonte = "ref"

    cargo_per_voyage = min(volume, float(dwt_cargo))
    num_voyages      = math.ceil(volume / dwt_cargo)

    speed_knots  = 12
    sea_days     = dist_nm / (speed_knots * 24)
    ballast_days = sea_days * 0.95   # lastro: ligeiramente mais rápido

    # Resolve chave curta do destino (ex: "Vietnã — Ho Chi Minh" → "Vietnam")
    # Necessário porque _DISCH_RATES/_CONGESTION_DEST/_PDA_DISCH usam chaves curtas
    dest_key = _DEST_KEYS.get(destino, destino)   # fallback: usa label como está

    load_rate  = _LOAD_RATES.get(porto_code, 20_000)
    disch_rate = _DISCH_RATES.get(dest_key, 18_000)
    loading_days = cargo_per_voyage / load_rate
    disch_days   = cargo_per_voyage / disch_rate

    # Congestionamento: safra brasileira mar–ago aplica fator ×1.5 na origem
    month = datetime.now().month
    harvest_factor   = 1.5 if 3 <= month <= 8 else 1.0
    congestion_orig  = _CONGESTION_ORIGIN.get(porto_code, 4.0) * harvest_factor
    congestion_dest  = _CONGESTION_DEST.get(dest_key, 3.0)
    congestion_days  = congestion_orig + congestion_dest

    # ── VERSÃO MÍNIMA (somente perna de ida — sem lastro de retorno) ──────────
    total_days_min = sea_days + loading_days + disch_days + congestion_days
    fuel_sea_min   = sea_days * consumo_mar * bunker
    fuel_port_min  = (loading_days + disch_days + congestion_days) * consumo_port * bunker
    hire_min       = total_days_min * daily_hire
    pda_per_mt     = _PDA_LOAD.get(porto_code, 0.60) + _PDA_DISCH.get(dest_key, 0.60)
    pda_total      = pda_per_mt * cargo_per_voyage
    op_cost_min    = (fuel_sea_min + fuel_port_min + hire_min) * _MARGEM_ARMADOR
    freight_min_mt = (op_cost_min + pda_total) / cargo_per_voyage

    # ── VERSÃO COMPLETA (ida + lastro — custo real do armador) ───────────────
    total_days   = total_days_min + ballast_days
    fuel_loaded  = sea_days     * consumo_mar        * bunker
    fuel_ballast = ballast_days * consumo_mar * 0.85 * bunker
    fuel_port    = (loading_days + disch_days + congestion_days) * consumo_port * bunker
    hire         = total_days * daily_hire
    op_cost      = (fuel_loaded + fuel_ballast + fuel_port + hire) * _MARGEM_ARMADOR
    voyage_cost  = op_cost + pda_total
    freight_per_mt = voyage_cost / cargo_per_voyage

    return {
        "freight_per_mt":     round(freight_per_mt, 2),
        "freight_min_per_mt": round(freight_min_mt, 2),
        "dist_nm":            dist_nm,
        "cargo_per_voyage":   int(cargo_per_voyage),
        "num_voyages":        num_voyages,
        "sea_days":           round(sea_days, 1),
        "ballast_days":       round(ballast_days, 1),
        "loading_days":       round(loading_days, 1),
        "disch_days":         round(disch_days, 1),
        "congestion_orig":    round(congestion_orig, 1),
        "congestion_dest":    congestion_dest,
        "congestion_days":    round(congestion_days, 1),
        "total_days_min":     round(total_days_min, 1),
        "total_days":         round(total_days, 1),
        "fuel_sea":           round(fuel_loaded + fuel_ballast, 0),
        "fuel_sea_min":       round(fuel_sea_min, 0),
        "fuel_port":          round(fuel_port, 0),
        "hire":               round(hire, 0),
        "hire_min":           round(hire_min, 0),
        "hire_day":           daily_hire,
        "hire_fonte":         hire_fonte,
        "margem_armador":     _MARGEM_ARMADOR,
        "pda":                round(pda_total, 0),
        "op_cost":            round(op_cost, 0),
        "voyage_cost":        round(voyage_cost, 0),
        "bunker":             bunker,
        "harvest":            harvest_factor > 1.0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# DATA LAYER
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def _get_snapshot() -> dict:
    base = {"usd_brl": 5.75, "cbot_soy_usd_mt": 0.0,
            "cbot_corn_usd_mt": 0.0, "ice_sugar_usd_mt": 0.0,
            "bunker_vlsfo": 550.0, "daily_hire": 0, "ts": "—"}
    try:
        eng = get_engine()
        with eng.connect() as conn:
            try:
                row = conn.execute(sqlalchemy.text(
                    "SELECT usd_brl, cbot_soy_usd_mt, cbot_corn_usd_mt, "
                    "ice_sugar_usd_mt, bunker_vlsfo, timestamp, daily_hire "
                    "FROM market_snapshots ORDER BY timestamp DESC LIMIT 1"
                )).fetchone()
                if row:
                    base = {
                        "usd_brl":          row[0] or 5.75,
                        "cbot_soy_usd_mt":  row[1] or 0.0,
                        "cbot_corn_usd_mt": row[2] or 0.0,
                        "ice_sugar_usd_mt": row[3] or 0.0,
                        "bunker_vlsfo":     row[4] or 550.0,
                        "ts":               str(row[5])[:16],
                        "daily_hire":       int(row[6] or 0),
                    }
            except Exception:
                row = conn.execute(sqlalchemy.text(
                    "SELECT usd_brl, cbot_soy_usd_mt, cbot_corn_usd_mt, "
                    "ice_sugar_usd_mt, bunker_vlsfo, timestamp "
                    "FROM market_snapshots ORDER BY timestamp DESC LIMIT 1"
                )).fetchone()
                if row:
                    base.update({
                        "usd_brl":          row[0] or 5.75,
                        "cbot_soy_usd_mt":  row[1] or 0.0,
                        "cbot_corn_usd_mt": row[2] or 0.0,
                        "ice_sugar_usd_mt": row[3] or 0.0,
                        "bunker_vlsfo":     row[4] or 550.0,
                        "ts":               str(row[5])[:16],
                    })
    except Exception:
        pass

    # Fallback direto no yfinance se DB não tem dados de bolsas
    if base["cbot_soy_usd_mt"] == 0.0:
        try:
            import yfinance as _yf
            _soy  = float(_yf.Ticker("ZS=F").history(period="2d")["Close"].iloc[-1])
            _corn = float(_yf.Ticker("ZC=F").history(period="2d")["Close"].iloc[-1])
            _sug  = float(_yf.Ticker("SB=F").history(period="2d")["Close"].iloc[-1])
            _fx   = float(_yf.Ticker("USDBRL=X").history(period="2d")["Close"].iloc[-1])
            base["cbot_soy_usd_mt"]  = round((_soy  / 100) * 36.7437, 2)
            base["cbot_corn_usd_mt"] = round((_corn / 100) * 39.3680, 2)
            base["ice_sugar_usd_mt"] = round(_sug  * 22.0462, 2)
            base["usd_brl"]          = round(_fx, 4)
            base["ts"]               = "ao vivo"
        except Exception:
            pass

    return base


_MAX_PRECO_FISICO_DIAS = 30   # aceita dados históricos até 30 dias (fallback Supabase)


@st.cache_data(ttl=300)
def _get_precos_fisicos_batch(produto: str) -> dict:
    """1 query → {UF_upper: {"preco_brl_ton":..., "fonte":..., "ts":...}}
    Substitui as N chamadas individuais em _render_comparativo."""
    try:
        eng = get_engine()
        with eng.connect() as conn:
            rows = conn.execute(sqlalchemy.text(
                "SELECT UPPER(uf), preco_brl_ton, fonte, timestamp "
                "FROM tb_preco_fisico_raw "
                "WHERE UPPER(produto)=UPPER(:p) "
                "ORDER BY timestamp DESC"
            ), {"p": produto}).fetchall()
        result: dict = {}
        now = datetime.now()
        for uf_up, preco, fonte, ts in rows:
            if uf_up in result:
                continue          # já tem o mais recente para este UF
            if not preco:
                continue
            # Verifica TTL
            try:
                from datetime import timezone
                if hasattr(ts, "tzinfo") and ts.tzinfo:
                    age = (datetime.now(timezone.utc) - ts).days
                else:
                    age = (now - ts).days if ts else 999
                if age > _MAX_PRECO_FISICO_DIAS:
                    continue
            except Exception:
                pass
            result[uf_up] = {"preco_brl_ton": preco, "fonte": fonte, "ts": str(ts)[:16]}
        return result
    except Exception:
        return {}


def _get_preco_fisico(produto: str, uf: str) -> dict | None:
    try:
        eng = get_engine()
        with eng.connect() as conn:
            row = conn.execute(sqlalchemy.text(
                "SELECT preco_brl_ton, fonte, timestamp FROM tb_preco_fisico_raw "
                "WHERE UPPER(produto)=UPPER(:p) AND UPPER(uf)=UPPER(:u) "
                "ORDER BY timestamp DESC LIMIT 1"
            ), {"p": produto, "u": uf}).fetchone()
            if row and row[0]:
                ts = row[2]
                if ts:
                    try:
                        from datetime import timezone
                        if hasattr(ts, 'tzinfo') and ts.tzinfo:
                            age_days = (datetime.now(timezone.utc) - ts).days
                        else:
                            age_days = (datetime.now() - ts).days
                        if age_days > _MAX_PRECO_FISICO_DIAS:
                            return None   # stale — usa CBOT+basis
                    except Exception:
                        pass
                return {"preco_brl_ton": row[0], "fonte": row[1], "ts": str(ts)[:16]}
    except Exception:
        pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CÁLCULO STACK EXW → FOB
# ─────────────────────────────────────────────────────────────────────────────

def _calcular_stack(snap: dict, porto_meta: dict, produto_meta: dict,
                    volume: float, preco_fisico_brl_ton: float | None = None) -> dict:
    fx       = snap["usd_brl"] or 5.75
    cbot_key = produto_meta["cbot_key"]
    bolsa    = snap.get(cbot_key, 0.0) or 0.0
    sugar_pr = produto_meta.get("sugar_premium", 0.0)

    basis_key = produto_meta.get("basis_key", "basis_soy")
    basis     = porto_meta.get(basis_key, 10)

    elev   = porto_meta["elev_usd"]
    estiva = 2.0

    if preco_fisico_brl_ton and preco_fisico_brl_ton > 0:
        fob_usd = preco_fisico_brl_ton / fx
        exw_usd = fob_usd - elev - estiva
        fonte   = "mercado físico (DB)"
    else:
        exw_usd = bolsa + basis - elev - estiva + sugar_pr
        fob_usd = exw_usd + elev + estiva
        fonte   = "CBOT/ICE + basis referência"

    fob_usd += sugar_pr if preco_fisico_brl_ton else 0.0
    fob_usd  = max(fob_usd, 0.0)
    exw_usd  = max(exw_usd, 0.0)

    kg_saca  = produto_meta["kg_saca"]
    fob_brl  = fob_usd * fx
    fob_saca = fob_brl * kg_saca / 1000
    exw_brl  = exw_usd * fx
    exw_saca = exw_brl * kg_saca / 1000

    return {
        "bolsa":      round(bolsa, 2),
        "basis":      round(basis, 2),
        "sugar_pr":   round(sugar_pr, 2),
        "exw_usd":    round(exw_usd, 2),
        "exw_brl":    round(exw_brl, 2),
        "exw_saca":   round(exw_saca, 2),
        "elev_usd":   elev,
        "estiva_usd": estiva,
        "fob_usd":    round(fob_usd, 2),
        "fob_brl":    round(fob_brl, 2),
        "fob_saca":   round(fob_saca, 2),
        "total_usd":  round(fob_usd * volume, 0),
        "total_brl":  round(fob_brl * volume, 0),
        "fx":         fx,
        "fonte":      fonte,
        "volume":     volume,
        "kg_saca":    kg_saca,
    }


# ─────────────────────────────────────────────────────────────────────────────
# RENDER HELPERS — sem linhas em branco dentro dos blocos HTML
# ─────────────────────────────────────────────────────────────────────────────

def _badge(texto: str, cor: str) -> str:
    return (f'<span style="background:{cor}22;border:1px solid {cor}55;color:{cor};'
            f'padding:2px 10px;border-radius:20px;font-size:9px;font-weight:700;'
            f'letter-spacing:.8px;font-family:Montserrat,sans-serif">{texto}</span>')


def _render_ticker(snap: dict):
    items = [
        ("SOJA CBOT",    snap["cbot_soy_usd_mt"],  "USD/MT"),
        ("MILHO CBOT",   snap["cbot_corn_usd_mt"],  "USD/MT"),
        ("AÇÚCAR ICE",   snap["ice_sugar_usd_mt"],  "USD/MT"),
        ("BUNKER VLSFO", snap["bunker_vlsfo"],       "USD/MT"),
        ("USD/BRL",      snap["usd_brl"],             ""),
    ]
    cols = st.columns(5)
    for i, (lbl, val, unid) in enumerate(items):
        cor = "#FA8200" if val > 0 else "#555E6D"
        dot = f'<span style="display:inline-block;width:6px;height:6px;background:{cor};border-radius:50%;margin-right:5px;vertical-align:middle"></span>' if val > 0 else ""
        with cols[i]:
            st.markdown(
                f'<div style="background:#12141C;'
                f'border:1px solid #2A2E3F;border-radius:10px;padding:14px 12px;'
                f'text-align:center;font-family:Montserrat,sans-serif;'
                f'box-shadow:0 4px 16px rgba(0,0,0,.35)">'
                f'<div style="font-size:8px;font-weight:700;letter-spacing:1.5px;color:#4B5563;margin-bottom:7px">{dot}{lbl}</div>'
                f'<div style="font-size:22px;font-weight:900;color:{cor};line-height:1;letter-spacing:-.5px">{val:,.2f}</div>'
                f'<div style="font-size:9px;color:#2E3545;margin-top:4px;font-weight:600">{unid if unid else "&nbsp;"}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    ts  = snap["ts"]
    has = snap["cbot_soy_usd_mt"] > 0
    if has:
        st.caption(f"Cotações: {ts}")
    else:
        st.caption(f"Mercado sem dados ao vivo ({ts}). Execute o scraper para atualizar.")


def _render_stack_card(r: dict, porto_label: str, produto_meta: dict,
                        regiao: str, produto_key: str):
    prod_lbl  = produto_meta["label"]
    icumsa    = produto_meta.get("icumsa")
    sugar_pr  = r["sugar_pr"]
    icumsa_badge = f' {_badge("ICUMSA " + icumsa, "#9B59B6")}' if icumsa else ""
    sugar_pr_html = (
        f'<div style="font-size:11px;color:#9B59B6;margin-top:2px">+ ${sugar_pr:.2f}/MT premium sobre VHP</div>'
        if sugar_pr > 0 else ""
    )
    elev_row = (
        f'<div style="display:flex;align-items:center;gap:6px;margin-top:10px;padding-top:10px;'
        f'border-top:1px dashed #E8E9EC;flex-wrap:wrap">'
        f'<span style="font-size:10px;color:#BFBFBF;font-family:Montserrat,sans-serif">STACK FOB:</span>'
        f'<span style="font-size:10px;color:#1A1A1A;font-family:monospace">EXW ${r["exw_usd"]:,.2f}</span>'
        f'<span style="font-size:10px;color:#BFBFBF">+</span>'
        f'<span style="font-size:10px;color:#1A1A1A;font-family:monospace">Elevação ${r["elev_usd"]:.1f}</span>'
        f'<span style="font-size:10px;color:#BFBFBF">+</span>'
        f'<span style="font-size:10px;color:#1A1A1A;font-family:monospace">Estiva ${r["estiva_usd"]:.1f}</span>'
        f'<span style="font-size:10px;color:#BFBFBF">+</span>'
        f'<span style="font-size:10px;color:#FA8200;font-weight:700;font-family:monospace">FOB ${r["fob_usd"]:,.2f}</span>'
        f'</div>'
    )
    basis_cor = "#329632" if r["basis"] >= 0 else "#fa3232"
    basis_sg  = "+" if r["basis"] >= 0 else ""
    st.markdown(
        f'<div style="background:#FFFDF8;border:1px solid #F0E6D0;border-top:4px solid #FA8200;border-radius:12px;padding:24px 28px;margin:12px 0;box-shadow:0 4px 20px rgba(250,130,0,.08);font-family:Montserrat,sans-serif">'
        f'<div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap">'
        f'<div style="font-size:9px;font-weight:700;letter-spacing:2.5px;color:#FA8200">{prod_lbl.upper()} &nbsp;·&nbsp; FOB {porto_label.upper()}</div>'
        f'{_badge(regiao, "#64C8FA")}{icumsa_badge}'
        f'<div style="margin-left:auto;font-size:9px;color:#BFBFBF">{r["fonte"]}</div>'
        f'</div>'
        f'<div style="display:flex;align-items:flex-end;gap:32px;flex-wrap:wrap">'
        f'<div>'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#BFBFBF;margin-bottom:4px">PREÇO FOB</div>'
        f'<div style="font-size:46px;font-weight:900;color:#FA8200;line-height:1">$ {r["fob_usd"]:,.2f}</div>'
        f'<div style="font-size:13px;color:#7F7F7F;margin-top:4px">USD / MT &nbsp;<span style="font-size:12px;font-weight:700;color:{basis_cor}">{basis_sg}{r["basis"]:.1f} USD/MT basis</span></div>'
        f'<div style="font-size:11px;color:#BFBFBF;margin-top:2px">Bolsa {r["bolsa"]:.2f} · câmbio {r["fx"]:.4f}</div>'
        f'{sugar_pr_html}'
        f'</div>'
        f'<div style="border-left:1px solid #E8E9EC;padding-left:28px">'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#BFBFBF;margin-bottom:4px">EXW ORIGEM</div>'
        f'<div style="font-size:32px;font-weight:900;color:#555;line-height:1">$ {r["exw_usd"]:,.2f}</div>'
        f'<div style="font-size:13px;color:#7F7F7F;margin-top:4px">USD / MT (ex-works)</div>'
        f'<div style="font-size:11px;color:#BFBFBF;margin-top:2px">R$ {r["exw_brl"]:,.2f}/MT · R$ {r["exw_saca"]:,.2f}/sc</div>'
        f'</div>'
        f'<div style="border-left:1px solid #E8E9EC;padding-left:28px">'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#BFBFBF;margin-bottom:4px">EM REAIS</div>'
        f'<div style="font-size:32px;font-weight:900;color:#1A1A1A;line-height:1">R$ {r["fob_brl"]:,.2f}</div>'
        f'<div style="font-size:13px;color:#7F7F7F;margin-top:4px">BRL / MT</div>'
        f'</div>'
        f'<div style="border-left:1px solid #E8E9EC;padding-left:28px">'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#BFBFBF;margin-bottom:4px">POR SACA ({r["kg_saca"]} KG)</div>'
        f'<div style="font-size:32px;font-weight:900;color:#1A1A1A;line-height:1">R$ {r["fob_saca"]:,.2f}</div>'
        f'<div style="font-size:13px;color:#7F7F7F;margin-top:4px">BRL / saca</div>'
        f'</div>'
        f'<div style="border-left:1px solid #E8E9EC;padding-left:28px">'
        f'<div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#BFBFBF;margin-bottom:4px">TOTAL ({r["volume"]:,.0f} MT)</div>'
        f'<div style="font-size:28px;font-weight:900;color:#1A1A1A;line-height:1">$ {r["total_usd"]/1_000_000:,.2f}M</div>'
        f'<div style="font-size:13px;color:#7F7F7F;margin-top:4px">USD</div>'
        f'</div>'
        f'</div>'
        f'{elev_row}'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_comparativo(snap: dict, produto_key: str, produto_meta: dict,
                         volume: float, porto_atual: str):
    st.markdown(
        '<div style="font-size:9px;font-weight:700;letter-spacing:2px;color:#FA8200;margin:20px 0 10px">COMPARATIVO — TODOS OS PORTOS</div>',
        unsafe_allow_html=True,
    )
    is_sugar = produto_key.startswith("SUGAR")
    portos_filtrados = {
        k: v for k, v in _PORTOS.items()
        if not is_sugar or v.get("sugar_porto", False) or k == porto_atual
    }
    # Uma única query batch em vez de N queries individuais
    precos_batch = _get_precos_fisicos_batch(produto_key)
    rows_html = ""
    for label, meta in portos_filtrados.items():
        pf = precos_batch.get((meta["uf"] or "").upper())
        r  = _calcular_stack(snap, meta, produto_meta, volume,
                             pf["preco_brl_ton"] if pf else None)
        destaque = label == porto_atual
        bg  = "#FFF3E3" if destaque else "#fff"
        fw  = "800"    if destaque else "500"
        bdr = "border-left:4px solid #FA8200;" if destaque else "border-left:4px solid transparent;"
        basis_txt = f'{r["basis"]:+.1f}' if r["basis"] != 0 else "—"
        obs_txt   = meta.get("obs", "")
        obs_html  = f'<span style="font-size:9px;color:#9B59B6;margin-left:6px">{obs_txt}</span>' if obs_txt else ""
        rows_html += (
            f'<tr style="background:{bg};{bdr}">'
            f'<td style="padding:8px 14px;font-size:12px;font-weight:{fw};color:#1A1A1A">{"→ " if destaque else ""}{label}{obs_html}</td>'
            f'<td style="padding:8px 14px;font-size:10px">{_badge(meta["regiao"],"#64C8FA")}</td>'
            f'<td style="padding:8px 14px;font-family:monospace;font-size:13px;font-weight:700;color:#FA8200;text-align:right">$ {r["fob_usd"]:,.2f}</td>'
            f'<td style="padding:8px 14px;font-family:monospace;font-size:12px;color:#555;text-align:right">$ {r["exw_usd"]:,.2f}</td>'
            f'<td style="padding:8px 14px;font-family:monospace;font-size:12px;color:#1A1A1A;text-align:right">R$ {r["fob_saca"]:,.2f}/sc</td>'
            f'<td style="padding:8px 14px;font-size:11px;color:#7F7F7F;text-align:right">{basis_txt}</td>'
            f'</tr>'
        )
    st.markdown(
        f'<div style="background:#fff;border:1px solid #EDE8E0;border-radius:10px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.06)">'
        f'<table style="width:100%;border-collapse:collapse">'
        f'<thead><tr style="background:#1A1D2A;border-bottom:2px solid #FA8200">'
        f'<th style="padding:10px 14px;text-align:left;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#7A8299">PORTO</th>'
        f'<th style="padding:10px 14px;text-align:left;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#7A8299">REGIÃO</th>'
        f'<th style="padding:10px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#FA8200">FOB USD/MT</th>'
        f'<th style="padding:10px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#7A8299">EXW USD/MT</th>'
        f'<th style="padding:10px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#7A8299">FOB R$/SC</th>'
        f'<th style="padding:10px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#7A8299">BASIS</th>'
        f'</tr></thead>'
        f'<tbody>{rows_html}</tbody>'
        f'</table>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _render_cif(snap: dict, resultado: dict, porto_code: str,
                produto_meta: dict, volume: float, navio_key: str):
    """Projeção CIF com frete calculado a partir de primeiros princípios."""
    with st.expander("Projetar CIF destino →", expanded=False):
        st.markdown(
            '<div style="background:#FFFBF5;border:1px solid #FAE0C0;border-radius:8px;'
            'padding:12px 16px;margin-bottom:14px;font-family:Montserrat,sans-serif;'
            'font-size:11px;color:#7A5000">'
            '<b>FOB</b>: risco de demurrage = originador &nbsp;·&nbsp; '
            '<b>CIF</b>: vendedor controla o embarque → sem risco demurrage para comprador'
            ' → <b>CIF geralmente mais acessível ao comprador</b>'
            '</div>',
            unsafe_allow_html=True,
        )

        # Seleção 2 níveis: região → destino (evita lista plana com 28 itens)
        _col_reg, _col_dest = st.columns([1, 2])
        with _col_reg:
            _regiao_cif = st.radio(
                "Região",
                list(_DEST_GRUPOS.keys()),
                key="px_cif_region",
                horizontal=False,
            )
        with _col_dest:
            destino = st.selectbox(
                "Destino CIF",
                _DEST_GRUPOS[_regiao_cif],
                key=f"px_cif_dest_{_regiao_cif}",
            )

        bunker           = snap.get("bunker_vlsfo", 550.0) or 550.0
        daily_hire_mkt   = snap.get("daily_hire", 0) or 0
        fr = _calcular_frete_maritimo(porto_code, destino, volume, navio_key, bunker, daily_hire_mkt)

        fob      = resultado["fob_usd"]
        frete    = fr["freight_per_mt"]
        frete_mn = fr["freight_min_per_mt"]
        seg      = round(fob * 0.003, 2)
        dem_cr   = _DEMURRAGE_CIF_CREDITO_USD
        cif      = fob + frete    + seg - dem_cr
        cif_min  = fob + frete_mn + seg - dem_cr
        fx       = resultado["fx"]
        kg       = resultado["kg_saca"]
        spread   = cif - fob
        cif_brl  = cif * fx
        cif_sc   = cif_brl * kg / 1000
        lastro_delta = cif - cif_min   # contribuição do lastro ao preço

        n_nav = fr["num_voyages"]
        if n_nav > 1:
            navio_curto = navio_key.split("(")[0].strip()
            st.info(
                f"Volume {volume:,.0f} MT → **{n_nav} viagens** de "
                f"{fr['cargo_per_voyage']:,.0f} MT cada ({navio_curto})"
            )

        ks  = "background:#1A1A1A;border-radius:8px;padding:14px 16px;text-align:center;font-family:Montserrat,sans-serif"
        ks2 = "background:#2A2A2A;border-radius:8px;padding:14px 16px;text-align:center;font-family:Montserrat,sans-serif;border:1px dashed #444"
        ca, cb, cc, cd = st.columns(4)
        with ca:
            st.markdown(
                f'<div style="{ks}">'
                f'<div style="font-size:9px;color:#BFBFBF;letter-spacing:1.5px;font-weight:700">CIF MÍNIMO (ida)</div>'
                f'<div style="font-size:28px;font-weight:900;color:#FA8200">$ {cif_min:,.2f}</div>'
                f'<div style="font-size:11px;color:#7F7F7F">{fr["total_days_min"]:.0f} dias · sem lastro</div>'
                f'</div>', unsafe_allow_html=True)
        with cb:
            st.markdown(
                f'<div style="{ks2}">'
                f'<div style="font-size:9px;color:#BFBFBF;letter-spacing:1.5px;font-weight:700">CIF C/ LASTRO (real)</div>'
                f'<div style="font-size:28px;font-weight:900;color:#FFC87A">$ {cif:,.2f}</div>'
                f'<div style="font-size:11px;color:#7F7F7F">{fr["total_days"]:.0f} dias · +$ {lastro_delta:.2f} lastro</div>'
                f'</div>', unsafe_allow_html=True)
        with cc:
            st.markdown(
                f'<div style="{ks}">'
                f'<div style="font-size:9px;color:#BFBFBF;letter-spacing:1.5px;font-weight:700">SPREAD FOB→CIF MIN</div>'
                f'<div style="font-size:28px;font-weight:900;color:#64C8FA">+ $ {cif_min - fob:,.2f}</div>'
                f'<div style="font-size:11px;color:#7F7F7F">USD / MT</div>'
                f'</div>', unsafe_allow_html=True)
        with cd:
            st.markdown(
                f'<div style="{ks}">'
                f'<div style="font-size:9px;color:#BFBFBF;letter-spacing:1.5px;font-weight:700">CIF MÍN EM REAIS</div>'
                f'<div style="font-size:22px;font-weight:900;color:#fff">R$ {cif_min * fx * kg / 1000:,.2f}/sc</div>'
                f'<div style="font-size:11px;color:#7F7F7F">R$ {cif_brl:,.2f}/MT</div>'
                f'</div>', unsafe_allow_html=True)

        # Valores por MT para o breakdown (versão mínima e completa)
        fuel_min_pm  = round((fr["fuel_sea_min"] + fr["fuel_port"]) / fr["cargo_per_voyage"], 2)
        fuel_full_pm = round((fr["fuel_sea"]     + fr["fuel_port"]) / fr["cargo_per_voyage"], 2)
        hire_min_pm  = round(fr["hire_min"] / fr["cargo_per_voyage"], 2)
        hire_full_pm = round(fr["hire"]     / fr["cargo_per_voyage"], 2)
        pda_pm       = round(fr["pda"]      / fr["cargo_per_voyage"], 2)
        navio_c      = navio_key.split("(")[0].strip()
        hire_lbl     = f'${fr["hire_day"]:,}/dia · {fr["hire_fonte"].upper()}'
        harvest_lbl  = " · 🌾 safra" if fr["harvest"] else ""
        congestion_pct = int((_MARGEM_ARMADOR - 1) * 100)
        ballast_hire_contrib = round((hire_full_pm - hire_min_pm) + (fuel_full_pm - fuel_min_pm), 2)

        st.markdown(
            f'<div style="margin-top:12px;background:#F9F9FB;border:1px solid #E8E9EC;border-radius:8px;padding:14px 18px;font-family:Montserrat,sans-serif;font-size:11px">'
            f'<div style="font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF;margin-bottom:10px">FORMAÇÃO CIF — DETALHAMENTO</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">FOB porto</span>'
            f'<span style="font-family:monospace;font-weight:700;color:#FA8200">$ {fob:,.2f} / MT</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Rota ({fr["dist_nm"]:,} NM)</span>'
            f'<span style="font-family:monospace;font-size:10px;color:#555">{navio_c} · {fr["cargo_per_voyage"]:,} MT/viagem · {fr["num_voyages"]}×</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Mar ida</span>'
            f'<span style="font-family:monospace;font-size:10px;color:#555">{fr["sea_days"]:.1f} dias</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Porto operacional (carga+desca.)</span>'
            f'<span style="font-family:monospace;font-size:10px;color:#555">{fr["loading_days"]:.1f} + {fr["disch_days"]:.1f} = {fr["loading_days"]+fr["disch_days"]:.1f} dias</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Congestionamento (fundeio{harvest_lbl})</span>'
            f'<span style="font-family:monospace;font-size:10px;color:#555">{fr["congestion_orig"]:.1f} orig + {fr["congestion_dest"]:.1f} dest = {fr["congestion_days"]:.1f} dias</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Bunker VLSFO (${bunker:.0f}/MT · ida)</span>'
            f'<span style="font-family:monospace;color:#1A1A1A">+ $ {fuel_min_pm:,.2f} / MT</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Hire ({hire_lbl} · {fr["total_days_min"]:.0f}d ida)</span>'
            f'<span style="font-family:monospace;color:#1A1A1A">+ $ {hire_min_pm:,.2f} / MT</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Margem armador (+{congestion_pct}% bunker+hire)</span>'
            f'<span style="font-family:monospace;font-size:10px;color:#E07000">× {_MARGEM_ARMADOR}</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">PDA origem + destino</span>'
            f'<span style="font-family:monospace;color:#1A1A1A">+ $ {pda_pm:,.2f} / MT</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Seguro (0,3% FOB)</span>'
            f'<span style="font-family:monospace;color:#1A1A1A">+ $ {seg:,.2f} / MT</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0">'
            f'<span style="color:#BFBFBF;width:240px">Crédito demurrage CIF</span>'
            f'<span style="font-family:monospace;color:#329632">− $ {dem_cr:.2f} / MT</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:7px 0;border-bottom:1px solid #E8E9EC;margin-top:2px">'
            f'<span style="font-weight:800;color:#FA8200;width:240px">= CIF Mínimo (ida) — {fr["total_days_min"]:.0f} dias</span>'
            f'<span style="font-family:monospace;font-weight:900;color:#FA8200;font-size:15px">$ {cif_min:,.2f} / MT</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:5px 0;border-bottom:1px solid #F0F0F0;background:#F5F5F5;margin:0 -18px;padding-left:18px">'
            f'<span style="color:#888;width:240px">+ Lastro retorno ({fr["ballast_days"]:.1f} dias)</span>'
            f'<span style="font-family:monospace;font-size:10px;color:#888">bunker + hire = + $ {ballast_hire_contrib:,.2f} / MT × {_MARGEM_ARMADOR}</span>'
            f'</div>'
            f'<div style="display:flex;gap:8px;align-items:center;padding:7px 0;margin-top:2px">'
            f'<span style="font-weight:800;color:#FFC87A;width:240px">= CIF c/ Lastro (real) — {fr["total_days"]:.0f} dias</span>'
            f'<span style="font-family:monospace;font-weight:900;color:#FFC87A;font-size:15px">$ {cif:,.2f} / MT</span>'
            f'</div>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

# st.fragment isola reruns: mudar input dentro do fragment NAO reroda toda app.
# Em Streamlit >= 1.37; fallback no-op para versoes antigas.
_fragment_decorator = getattr(st, "fragment", None) or getattr(st, "experimental_fragment", None)


def _wrap_fragment(fn):
    return _fragment_decorator(fn) if _fragment_decorator is not None else fn


@_wrap_fragment
def _frag_quick_quote():
    try:
        from dashboards.quick_quote_chat import render_quick_quote
        render_quick_quote()
    except Exception as _e:
        st.error(f"Quick Quote indisponivel: {_e}")


@_wrap_fragment
def _frag_calculadora():
    _render_calculadora()


@_wrap_fragment
def _frag_cotacao():
    try:
        from dashboards.cotacao_widget import render_cotacao_tab
        render_cotacao_tab()
    except Exception as _e:
        st.error(f"Modulo de Cotacao indisponivel: {_e}")


def render_pricing_tab():
    """
    Sub-abas do fluxo de formação de preço (cada uma isolada em st.fragment
    para que mudancas internas nao re-rodem a app inteira):
      1. Cotacao Rapida  — chat conversacional, resposta imediata
      2. Calculadora     — form completo com todos os parâmetros
      3. Cotação / PDF   — gerador de proposta formal (cotacao_widget)
    """
    tab_qq, tab_calc, tab_cot = st.tabs([
        "Cotacao Rapida",
        "Calculadora Detalhada",
        "Cotacao / Proposta",
    ])

    with tab_qq:
        _frag_quick_quote()

    with tab_calc:
        _frag_calculadora()

    with tab_cot:
        _frag_cotacao()


def _render_calculadora():
    st.markdown(
        '<div class="section-title">PREÇO EXW / FOB / CIF — CONSULTA DE MERCADO</div>',
        unsafe_allow_html=True,
    )

    snap = _get_snapshot()
    _render_ticker(snap)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns([1.8, 2.5, 1, 1])
    with c1:
        produto_key = st.selectbox(
            "Produto", list(_PRODUTOS.keys()),
            format_func=lambda k: _PRODUTOS[k]["label"],
            key="px_prod",
        )
    with c2:
        is_sugar = produto_key.startswith("SUGAR")
        porto_opts = (
            [k for k, v in _PORTOS.items() if v.get("sugar_porto", False)]
            if is_sugar else list(_PORTOS.keys())
        )
        porto_label = st.selectbox("Porto de saída", porto_opts, key="px_porto")
    with c3:
        volume = st.number_input(
            "Volume (MT)", min_value=100.0, value=25_000.0,
            step=1_000.0, key="px_vol",
        )
    with c4:
        porto_meta = _PORTOS[porto_label]
        porto_code = porto_meta["code"]
        navio_auto = _navio_para_porto_volume(volume, porto_code)
        # Key inclui o navio auto-selecionado: quando o volume cruzar o limiar de
        # classe (ex: 32k→Supramax), o key muda e o selectbox reseta para o novo default.
        navio_key  = st.selectbox(
            "Classe de navio ✦",
            list(_NAVIOS.keys()),
            index=list(_NAVIOS.keys()).index(navio_auto),
            key=f"px_navio_{porto_code}_{navio_auto.split()[0]}",
            help=f"Auto-selecionado por volume ({volume:,.0f} MT) e calado do porto. Você pode sobrescrever.",
        )

    # Nota sobre número de navios necessários + validação DWT do porto
    dwt_sel    = _NAVIOS[navio_key]["dwt_cargo"]
    dwt_max_p  = _PORTO_MAX_DWT.get(porto_code, 80_000)
    n_navios   = math.ceil(volume / dwt_sel)
    sh_sel     = navio_key.split("(")[0].strip()
    sh_auto    = navio_auto.split("(")[0].strip()
    navio_ok   = _NAVIOS[navio_key]["dwt_max"] <= dwt_max_p
    alerta_dwt = (
        f' &nbsp;<span style="color:#FA3232;font-weight:700">⚠ {porto_label.split(" (")[0]} aceita até '
        f'{dwt_max_p:,} DWT</span>'
        if not navio_ok else ""
    )
    if n_navios > 1:
        nota_navio = (
            f'<span style="color:#FA8200;font-weight:700">{n_navios}× {sh_sel}</span>'
            f' necessários para {volume:,.0f} MT{alerta_dwt}'
        )
    else:
        cor_auto = "#64C8FA" if navio_key == navio_auto else "#9B59B6"
        auto_txt = "✦ recomendado" if navio_key == navio_auto else f"⚡ recomendado: {sh_auto}"
        nota_navio = (
            f'1× {sh_sel} cobre {volume:,.0f} MT em 1 viagem'
            f' &nbsp;<span style="color:{cor_auto}">{auto_txt}</span>'
            f'{alerta_dwt}'
        )
    st.markdown(
        f'<div style="font-size:11px;color:#555;font-family:Montserrat,sans-serif;'
        f'margin-bottom:8px">{nota_navio}</div>',
        unsafe_allow_html=True,
    )

    produto_meta = _PRODUTOS[produto_key]
    regiao       = porto_meta["regiao"]

    pf        = _get_preco_fisico(produto_key, porto_meta["uf"])
    resultado = _calcular_stack(
        snap, porto_meta, produto_meta, volume,
        pf["preco_brl_ton"] if pf else None,
    )

    _render_stack_card(
        resultado,
        porto_label.split(" (")[0],
        produto_meta,
        regiao,
        produto_key,
    )

    if pf:
        st.caption(f"Fonte: {pf['fonte']} · {pf['ts']}")
    else:
        st.caption("Sem preço físico cadastrado — usando CBOT/ICE + basis referência.")

    _render_comparativo(snap, produto_key, produto_meta, volume, porto_label)

    _render_cif(snap, resultado, porto_code, produto_meta, volume, navio_key)
