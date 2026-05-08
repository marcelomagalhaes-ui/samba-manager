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
    # ARCO NORTE
    "Outeiro (Belém/PA)": {
        "code": "BELEM",   "uf": "PA", "regiao": "Arco Norte",
        "basis_soy": 12,   "basis_corn": 10, "basis_sugar": 43,
        "elev_usd": 14.0,  "sugar_porto": False,
    },
    "Barcarena (PA)": {
        "code": "BARCARENA","uf": "PA", "regiao": "Arco Norte",
        "basis_soy": 11,   "basis_corn": 9,  "basis_sugar": 42,
        "elev_usd": 15.0,  "sugar_porto": False,
    },
    "Santarém (PA)": {
        "code": "SANTAREM","uf": "PA", "regiao": "Arco Norte",
        "basis_soy": 10,   "basis_corn": 8,  "basis_sugar": 41,
        "elev_usd": 14.5,  "sugar_porto": False,
    },
    "Miritituba (PA)": {
        "code": "MIRITITUBA","uf":"PA","regiao": "Arco Norte",
        "basis_soy": 10,   "basis_corn": 8,  "basis_sugar": 40,
        "elev_usd": 5.0,   "sugar_porto": False,
        "obs": "Transbordo hidroviário",
    },
    "Itacoatiara (AM)": {
        "code": "ITACOATIARA","uf":"AM","regiao": "Arco Norte",
        "basis_soy": 9,    "basis_corn": 7,  "basis_sugar": 39,
        "elev_usd": 10.0,  "sugar_porto": False,
    },
    "Itaqui (MA)": {
        "code": "ITAQUI",  "uf": "MA", "regiao": "Nordeste",
        "basis_soy": 8,    "basis_corn": 7,  "basis_sugar": 42,
        "elev_usd": 11.0,  "sugar_porto": False,
    },
    # SUL / SUDESTE
    "Paranaguá (PR)": {
        "code": "PARANAGUA","uf": "PR", "regiao": "Sul",
        "basis_soy": 5,    "basis_corn": 4,  "basis_sugar": 38,
        "elev_usd": 13.0,  "sugar_porto": True,
    },
    "Santos (SP)": {
        "code": "SANTOS",  "uf": "SP", "regiao": "Sudeste",
        "basis_soy": 6,    "basis_corn": 5,  "basis_sugar": 40,
        "elev_usd": 14.0,  "sugar_porto": True,
    },
    "São Francisco (SC)": {
        "code": "SAO_FRANCISCO_SUL","uf":"SC","regiao":"Sul",
        "basis_soy": 4,    "basis_corn": 3,  "basis_sugar": 36,
        "elev_usd": 12.5,  "sugar_porto": False,
    },
    "Rio Grande (RS)": {
        "code": "RIO_GRANDE","uf": "RS", "regiao": "Sul",
        "basis_soy": 3,    "basis_corn": 3,  "basis_sugar": 36,
        "elev_usd": 13.5,  "sugar_porto": False,
    },
    "Vitória (ES)": {
        "code": "VITORIA", "uf": "ES", "regiao": "Sudeste",
        "basis_soy": 5,    "basis_corn": 4,  "basis_sugar": 38,
        "elev_usd": 12.0,  "sugar_porto": False,
    },
    # NORDESTE / AÇÚCAR
    "Maceió (AL)": {
        "code": "MACEIO",  "uf": "AL", "regiao": "Nordeste",
        "basis_soy": 6,    "basis_corn": 5,  "basis_sugar": 46,
        "elev_usd": 12.0,  "sugar_porto": True,
        "obs": "Principal porto açúcar VHP/IC45",
    },
    "Recife (PE)": {
        "code": "RECIFE",  "uf": "PE", "regiao": "Nordeste",
        "basis_soy": 6,    "basis_corn": 5,  "basis_sugar": 46,
        "elev_usd": 12.0,  "sugar_porto": True,
    },
    "Salvador (BA)": {
        "code": "SALVADOR","uf": "BA", "regiao": "Nordeste",
        "basis_soy": 6,    "basis_corn": 5,  "basis_sugar": 42,
        "elev_usd": 14.0,  "sugar_porto": False,
    },
    "Pecém (CE)": {
        "code": "PECEM",   "uf": "CE", "regiao": "Nordeste",
        "basis_soy": 7,    "basis_corn": 6,  "basis_sugar": 43,
        "elev_usd": 12.0,  "sugar_porto": False,
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
# DISTÂNCIAS MARÍTIMAS — novas_rotas_maritimas.csv (rota mais curta por destino)
# Bases: SANTOS / PARANAGUA / RIO_GRANDE / VITORIA / ITAQUI / BARCARENA / PECEM
# Derivados: estimativa geográfica para demais portos
# ─────────────────────────────────────────────────────────────────────────────
_DIST_BASE = {
    # porto_csv_code → {destino_zone → dist_nm}
    "SANTOS": {
        "China": 11000, "Vietna": 10750, "Indonesia": 10950, "India": 8300,
        "OME": 7200, "Europa": 5600, "Egito": 5400, "Africa": 4400, "EUA": 4000,
    },
    "PARANAGUA": {
        "China": 11150, "Vietna": 10900, "Indonesia": 11100, "India": 8450,
        "OME": 7350, "Europa": 5750, "Egito": 5550, "Africa": 4550, "EUA": 4150,
    },
    "RIO_GRANDE": {
        "China": 11300, "Vietna": 11050, "Indonesia": 11250, "India": 8800,
        "OME": 7500, "Europa": 5900, "Egito": 5700, "Africa": 4700, "EUA": 4300,
    },
    "VITORIA": {
        "China": 10550, "Vietna": 10300, "Indonesia": 10550, "India": 8000,
        "OME": 6900, "Europa": 5300, "Egito": 5100, "Africa": 4100, "EUA": 3700,
    },
    "ITAQUI": {
        "China": 10350, "Vietna": 10100, "Indonesia": 10300, "India": 7600,
        "OME": 6600, "Europa": 5000, "Egito": 4800, "Africa": 3800, "EUA": 3400,
    },
    "BARCARENA": {
        "China": 10300, "Vietna": 10050, "Indonesia": 10250, "India": 7550,
        "OME": 6650, "Europa": 5050, "Egito": 4850, "Africa": 3750, "EUA": 3300,
    },
    "PECEM": {
        "China": 10000, "Vietna": 9700, "Indonesia": 9950, "India": 7300,
        "OME": 6400, "Europa": 4800, "Egito": 4600, "Africa": 3550, "EUA": 3100,
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

# Destino CIF label → chave interna
_DEST_KEYS = {
    "China":              "China",
    "Vietnã":             "Vietna",
    "Indonésia":          "Indonesia",
    "Índia":              "India",
    "Oriente Médio":      "OME",
    "Europa NW":          "Europa",
    "Egito / N. África":  "Egito",
    "África Subsaariana": "Africa",
    "EUA / Golfo":        "EUA",
}

_DESTINOS_CIF = list(_DEST_KEYS.keys())

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
    "China":              20_000,
    "Vietnã":             15_000,
    "Indonésia":          15_000,
    "Índia":              18_000,
    "Oriente Médio":      15_000,
    "Europa NW":          25_000,
    "Egito / N. África":  15_000,
    "África Subsaariana": 12_000,
    "EUA / Golfo":        25_000,
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
    "China":              0.70,
    "Vietnã":             0.55,
    "Indonésia":          0.55,
    "Índia":              0.60,
    "Oriente Médio":      0.45,
    "Europa NW":          0.60,
    "Egito / N. África":  0.50,
    "África Subsaariana": 0.45,
    "EUA / Golfo":        0.65,
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
    dest_key = _DEST_KEYS.get(destino, "China")
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

    load_rate  = _LOAD_RATES.get(porto_code, 20_000)
    disch_rate = _DISCH_RATES.get(destino, 18_000)
    loading_days = cargo_per_voyage / load_rate
    disch_days   = cargo_per_voyage / disch_rate

    # Congestionamento: safra brasileira mar–ago aplica fator ×1.5 na origem
    month = datetime.now().month
    harvest_factor   = 1.5 if 3 <= month <= 8 else 1.0
    congestion_orig  = _CONGESTION_ORIGIN.get(porto_code, 4.0) * harvest_factor
    congestion_dest  = _CONGESTION_DEST.get(destino, 3.0)
    congestion_days  = congestion_orig + congestion_dest

    # ── VERSÃO MÍNIMA (somente perna de ida — sem lastro de retorno) ──────────
    total_days_min = sea_days + loading_days + disch_days + congestion_days
    fuel_sea_min   = sea_days * consumo_mar * bunker
    fuel_port_min  = (loading_days + disch_days + congestion_days) * consumo_port * bunker
    hire_min       = total_days_min * daily_hire
    pda_per_mt     = _PDA_LOAD.get(porto_code, 0.60) + _PDA_DISCH.get(destino, 0.60)
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

def _get_snapshot() -> dict:
    base = {"usd_brl": 5.75, "cbot_soy_usd_mt": 0.0,
            "cbot_corn_usd_mt": 0.0, "ice_sugar_usd_mt": 0.0,
            "bunker_vlsfo": 550.0, "daily_hire": 0, "ts": "—"}
    try:
        eng = get_engine()
        with eng.connect() as conn:
            # Tenta buscar daily_hire (BDI × 12); ignora se coluna não existir
            try:
                row = conn.execute(sqlalchemy.text(
                    "SELECT usd_brl, cbot_soy_usd_mt, cbot_corn_usd_mt, "
                    "ice_sugar_usd_mt, bunker_vlsfo, timestamp, daily_hire "
                    "FROM market_snapshots ORDER BY timestamp DESC LIMIT 1"
                )).fetchone()
                if row:
                    return {
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
                    return base
    except Exception:
        pass
    return base


_MAX_PRECO_FISICO_DIAS = 3   # rejeita preço físico com mais de 3 dias

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
        cor = "#FA8200" if val > 0 else "#BFBFBF"
        with cols[i]:
            st.markdown(
                f'<div style="background:#fff;border:1px solid #E8E9EC;border-radius:8px;padding:10px 12px;text-align:center;font-family:Montserrat,sans-serif">'
                f'<div style="font-size:9px;font-weight:700;letter-spacing:1px;color:#BFBFBF;margin-bottom:4px">{lbl}</div>'
                f'<div style="font-size:16px;font-weight:900;color:{cor}">{val:,.2f}</div>'
                f'<div style="font-size:9px;color:#BFBFBF">{unid}</div>'
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
        f'<div style="background:#fff;border:1px solid #E8E9EC;border-top:4px solid #FA8200;border-radius:12px;padding:24px 28px;margin:12px 0;box-shadow:0 2px 10px rgba(0,0,0,.06);font-family:Montserrat,sans-serif">'
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
    rows_html = ""
    for label, meta in portos_filtrados.items():
        pf = _get_preco_fisico(produto_key, meta["uf"])
        r  = _calcular_stack(snap, meta, produto_meta, volume,
                             pf["preco_brl_ton"] if pf else None)
        destaque = label == porto_atual
        bg  = "#FFF8F0" if destaque else "#fff"
        fw  = "800"    if destaque else "500"
        bdr = "border-left:3px solid #FA8200;" if destaque else ""
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
        f'<div style="background:#fff;border:1px solid #E8E9EC;border-radius:10px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.04)">'
        f'<table style="width:100%;border-collapse:collapse">'
        f'<thead><tr style="background:#F9F9FB;border-bottom:1px solid #E8E9EC">'
        f'<th style="padding:9px 14px;text-align:left;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF">PORTO</th>'
        f'<th style="padding:9px 14px;text-align:left;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF">REGIÃO</th>'
        f'<th style="padding:9px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF">FOB USD/MT</th>'
        f'<th style="padding:9px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF">EXW USD/MT</th>'
        f'<th style="padding:9px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF">FOB R$/SC</th>'
        f'<th style="padding:9px 14px;text-align:right;font-size:9px;font-weight:700;letter-spacing:1.5px;color:#BFBFBF">BASIS</th>'
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

        destino = st.selectbox("Destino CIF", _DESTINOS_CIF, key="px_cif_dest")

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

def render_pricing_tab():
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
        navio_key  = st.selectbox(
            "Classe de navio", list(_NAVIOS.keys()),
            index=list(_NAVIOS.keys()).index(navio_auto),
            key=f"px_navio_{porto_label}",
        )

    # Nota sobre número de navios necessários
    dwt_sel    = _NAVIOS[navio_key]["dwt_cargo"]
    n_navios   = math.ceil(volume / dwt_sel)
    sh_sel     = navio_key.split("(")[0].strip()
    sh_auto    = navio_auto.split("(")[0].strip()
    if n_navios > 1:
        nota_navio = (
            f'<span style="color:#FA8200;font-weight:700">{n_navios}× {sh_sel}</span>'
            f' necessários para {volume:,.0f} MT'
        )
        if navio_key != navio_auto:
            nota_navio += f' &nbsp;·&nbsp; <span style="color:#64C8FA">recomendado: {sh_auto}</span>'
    elif navio_key != navio_auto:
        nota_navio = (
            f'1× {sh_sel} cobre {volume:,.0f} MT'
            f' &nbsp;·&nbsp; <span style="color:#BFBFBF">auto-selecionado: {sh_auto}</span>'
        )
    else:
        nota_navio = f'1× {sh_sel} cobre {volume:,.0f} MT em 1 viagem'
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
