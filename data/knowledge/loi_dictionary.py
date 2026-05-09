# -*- coding: utf-8 -*-
"""
data/knowledge/loi_dictionary.py
================================
Matriz do Agente Gerador de LOI — alinhada aos templates REAIS no Drive.

Padrão dos templates: a base é a COMMODITY (ex.: Soja). Dentro dela há
múltiplos PRODUTOS derivados (ex.: Yellow Soybean / Soybean Meal). O agente
mantém somente a linha/bloco do produto escolhido e apaga os demais.

Cada produto declara:
  label              : rótulo amigável ao usuário
  strict_keywords    : substrings únicas (identificam exclusivamente este produto)
  family_keywords    : substrings que podem ser compartilhadas (ex.: "Cotton Lint")
  packaging_options  : opções de embalagem disponíveis (Drive: dropdown filtrado)
  default_packaging  : opção padrão se templates a têm pré-selecionada
"""
from __future__ import annotations
from typing import Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────────────
# SOJA
# ─────────────────────────────────────────────────────────────────────────────
SOY = {
    "code": "SOY",
    "label_pt": "Soja",
    "label_en": "Soybean",
    "template_filename": "MODEL-LOI-SOY-SE",
    "products": [
        {
            "label":             "Yellow Soybean, GMO - Grade #2",
            "strict_keywords":   ["Yellow Soybean"],
            "family_keywords":   [],
            "packaging_options": ["Bulk", "1,000/1,200 kg Big Bags", "50 kg PP Bags"],
        },
        {
            "label":             "Soybean Meal - Solvent Extracted",
            "strict_keywords":   ["Soybean Meal"],
            "family_keywords":   [],
            "packaging_options": ["Bulk", "1,000 kg Big Bags", "50 kg PP Bags"],
        },
    ],
    "incoterms":  ["FOB Santos", "FOB Outeiro-Belém", "CIF ASWP"],
    "extra_rules": {"price_basis": "CBOT"},
}

# ─────────────────────────────────────────────────────────────────────────────
# MILHO
# ─────────────────────────────────────────────────────────────────────────────
CORN = {
    "code": "CORN",
    "label_pt": "Milho",
    "label_en": "Corn",
    "template_filename": "MODEL-LOI-CORN-SE",
    "products": [
        {
            "label":             "Yellow Corn (Maize), GMO - Grade #2",
            "strict_keywords":   ["Yellow Corn"],
            "family_keywords":   [],
            "packaging_options": ["Bulk", "1,000/1,200 kg Big Bags", "25/50 kg PP Bags"],
        },
        {
            "label":             "Popcorn - Premium Butterfly/Mushroom Grade",
            "strict_keywords":   ["Popcorn"],
            "family_keywords":   [],
            "packaging_options": ["Containers"],
        },
        {
            "label":             "DDGS - Distillers Dried Grains with Solubles",
            "strict_keywords":   ["DDGS"],
            "family_keywords":   [],
            "packaging_options": ["Bulk", "1,000/1,200 kg Big Bags", "25/50 kg PP Bags"],
        },
    ],
    "incoterms":  ["FOB Santos", "FOB Outeiro-Belém", "CIF ASWP"],
    "extra_rules": {"price_basis": "CBOT"},
}

# ─────────────────────────────────────────────────────────────────────────────
# AÇÚCAR
# ─────────────────────────────────────────────────────────────────────────────
SUGAR = {
    "code": "SUGAR",
    "label_pt": "Açúcar",
    "label_en": "Sugar",
    "template_filename": "MODEL-LOI-SUGAR-SE",
    "products": [
        {
            "label":             "Refined White Sugar ICUMSA 45 - Grade A",
            # "Max 45 IU" identifica o bloco de parâmetros técnicos no template
            "strict_keywords":   ["ICUMSA 45", "IC45", "Max 45 IU"],
            "family_keywords":   [],
            "packaging_options": ["50 kg PP bags", "Big Bags", "Bulk"],
        },
        {
            "label":             "Refined White Sugar ICUMSA 150 - Grade A",
            "strict_keywords":   ["ICUMSA 150", "IC150", "Max 150 IU"],
            "family_keywords":   [],
            "packaging_options": ["50 kg PP bags", "Big Bags", "Bulk"],
        },
        {
            "label":             "Raw Cane Sugar VHP 600-1200 - Grade A",
            # "600-1200 IU" identifica parâmetros VHP; não aparece em IC45/IC150
            "strict_keywords":   ["VHP", "600-1200 IU"],
            "family_keywords":   [],
            "packaging_options": ["Bulk"],
        },
    ],
    "incoterms":  ["FOB Santos", "FOB Paranaguá", "CIF ASWP"],
    "extra_rules": {
        "origin_options": ["Brazil", "Thailand", "Outra"],
        "non_brazil_alert": "Revisar certificação MAPA — origem não-Brasil.",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# FRANGO
# Nota: cada corte é identificado pelo padrão "(Chicken X)" no template.
# ─────────────────────────────────────────────────────────────────────────────
CHICKEN = {
    "code": "CHICKEN",
    "label_pt": "Frango",
    "label_en": "Chicken",
    "template_filename": "MODEL-LOI-CHICKEN-SE",
    "products": [
        {"label": "Frozen Chicken Paws",                "strict_keywords": ["Chicken Paws"],         "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Frozen Chicken Feet",                "strict_keywords": ["Chicken Feet"],         "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Mid-Joint Wings (MJW)",              "strict_keywords": ["Mid-Joint Wings"],      "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Chicken Leg Quarters (CLQ)",         "strict_keywords": ["Chicken Leg Quarters"], "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Frozen Whole Chicken",               "strict_keywords": ["Whole Chicken"],        "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Boneless Skinless Chicken Breast",   "strict_keywords": ["Chicken Breast"],       "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Frozen Chicken Drumsticks",          "strict_keywords": ["Chicken Drumsticks"],   "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Three-Joint Wings (3JW)",            "strict_keywords": ["Three-Joint Wings"],    "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
        {"label": "Frozen Chicken Wing Tips",           "strict_keywords": ["Chicken Wing Tips"],    "family_keywords": [], "packaging_options": ["40HQ Reefer Containers"]},
    ],
    "incoterms":  ["CIF ASWP"],
    "extra_rules": {
        "volume_unit": "FCL 40HQ",
        "fixed_certifications": "HACCP · HALAL · MAPA · HPAI free status MAPA + WOAH/OIE · GACC",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# ÓLEOS VEGETAIS
# Cada óleo tem keyword único pela sigla (CDSO, RBD Soy, CCRO, RBD Corn, etc.)
# ─────────────────────────────────────────────────────────────────────────────
VEGOIL = {
    "code": "VEGOIL",
    "label_pt": "Óleos Vegetais",
    "label_en": "Vegetable Oils",
    "template_filename": "MODEL-LOI-VEGOIL-SE",
    "products": [
        # Crude
        # "Crude Soybean" cobre {Crude Soybean Oil - CDSO} no template
        {"label": "Crude Soybean Oil (CDSO)",      "grade": "Crude", "strict_keywords": ["CDSO", "Crude Soybean"],             "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums"]},
        {"label": "Crude Corn Oil (CCRO)",         "grade": "Crude", "strict_keywords": ["CCRO", "Crude Corn"],               "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums"]},
        {"label": "Crude Sunflower Oil (CSFO)",    "grade": "Crude", "strict_keywords": ["CSFO", "Crude Sunflower"],          "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums"]},
        # "Crude Palm" cobre {Crude Palm Oil - CPO}; "(CPO)" cobre {1511.10 (CPO)}
        {"label": "Crude Palm Oil (CPO)",          "grade": "Crude", "strict_keywords": ["CPO ", "(CPO)", "Palm CPO", "Crude Palm"], "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums"]},
        # RBD — "Refined Soybean/Corn/Sunflower" cobrem os marcadores de nome no template
        {"label": "Refined Soybean Oil (RBD)",     "grade": "RBD",   "strict_keywords": ["RBD Soy", "Soybean RBD", "Refined Soybean"],         "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums", "Jugs & Bottles"]},
        {"label": "Refined Corn Oil (RBD)",        "grade": "RBD",   "strict_keywords": ["RBD Corn", "Corn RBD", "Refined Corn"],               "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums", "Jugs & Bottles"]},
        {"label": "Refined Sunflower Oil (RBD)",   "grade": "RBD",   "strict_keywords": ["RBD Sunflower", "Sunflower RBD", "Refined Sunflower"], "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums", "Jugs & Bottles"]},
        {"label": "Refined Palm Oil (RBD PO)",     "grade": "RBD",   "strict_keywords": ["RBD Palm", "RBD PO", "Palm RBD", "Refined Palm"],     "family_keywords": [], "packaging_options": ["Flexitank", "ISOtank", "Drums", "Jugs & Bottles"]},
    ],
    "incoterms":  ["CIF ASWP", "FOB Santos", "FOB Paranaguá"],
    "extra_rules": {
        "origin_options": ["Brazil", "Ukraine", "Malaysia", "Thailand"],
        # Templates VegOil têm 2 blocos QUALITY_STANDARD: um {BRAZIL: ...} e
        # outro {ORIGIN_COUNTRY: ...}. O engine escolhe um conforme origem.
        "qs_brazil_marker":     "BRAZIL:",
        "qs_non_brazil_marker": "ORIGIN_COUNTRY:",
        "quality_standard_brazil":     "FOSFA 54 / AOCS / CODEX Alimentarius / ANEC 81 / PORAM",
        "quality_standard_other":      "FOSFA 54 / AOCS / CODEX Alimentarius / PORAM",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# ALGODÃO
# Cotton Lint variantes compartilham "Cotton Lint" como family keyword.
# ─────────────────────────────────────────────────────────────────────────────
COTTON = {
    "code": "COTTON",
    "label_pt": "Algodão",
    "label_en": "Cotton",
    "template_filename": "MODEL-LOI-COTTON-SE",
    "products": [
        {"label": "Cotton Lint - 31-1 Good Middling",   "product_type": "Cotton Lint", "strict_keywords": ["31-1"], "family_keywords": ["Cotton Lint"],
         "packaging_options": ["220/230 kg Bales in 40' HC"]},
        {"label": "Cotton Lint - 31-3 Middling",        "product_type": "Cotton Lint", "strict_keywords": ["31-3"], "family_keywords": ["Cotton Lint"],
         "packaging_options": ["220/230 kg Bales in 40' HC"]},
        {"label": "Cotton Lint - 41-4 Strict Low Mid",  "product_type": "Cotton Lint", "strict_keywords": ["41-4"], "family_keywords": ["Cotton Lint"],
         "packaging_options": ["220/230 kg Bales in 40' HC"]},
        {"label": "Cotton Lint - 51-5 Low Middling",    "product_type": "Cotton Lint", "strict_keywords": ["51-5"], "family_keywords": ["Cotton Lint"],
         "packaging_options": ["220/230 kg Bales in 40' HC"]},
        {"label": "Cottonseed Meal",                    "product_type": "Cottonseed Meal", "strict_keywords": ["Cottonseed Meal"], "family_keywords": [],
         "packaging_options": ["Bulk", "Container Operations"]},
    ],
    "incoterms":  ["CIF ASWP"],
    "extra_rules": {
        "hvi_transparency_clause": (
            "Seller warrants that all Cotton Lint bales are tested by High Volume Instrument (HVI) "
            "per USDA-AMS / ASTM D-1448 standards. A HVI Certificate (Color, Staple Length, "
            "Strength, Micronaire, and Uniformity) shall accompany each Bill of Lading."
        ),
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# ARROZ
# Note: White Rice e Parboiled Rice compartilham HS code 1006.30 (White/Parboiled).
# ─────────────────────────────────────────────────────────────────────────────
RICE = {
    "code": "RICE",
    "label_pt": "Arroz",
    "label_en": "Rice",
    "template_filename": "MODEL-LOI-RICE-SE",
    "products": [
        {"label": "Paddy / Husked Rice",              "strict_keywords": ["Paddy"],                            "family_keywords": [],
         "packaging_options": ["Bulk", "1,000 kg Big Bags", "50 kg PP Bags"]},
        {"label": "Brown Rice (Cargo)",               "strict_keywords": ["Brown"],                            "family_keywords": [],
         "packaging_options": ["Bulk", "1,000 kg Big Bags", "25/50 kg PP Bags"]},
        {"label": "White Rice (Polished) - Type 1",   "strict_keywords": ["White Rice", "White/Parboiled"],    "family_keywords": [],
         "packaging_options": ["Bulk", "1,000 kg Big Bags", "25/50 kg PP Bags", "Retail Packs"]},
        {"label": "Parboiled Rice",                   "strict_keywords": ["Parboiled", "White/Parboiled"],     "family_keywords": [],
         "packaging_options": ["Bulk", "1,000 kg Big Bags", "25/50 kg PP Bags", "Retail Packs"]},
        {"label": "Broken Rice (Trincas)",            "strict_keywords": ["Broken", "Trincas"],                "family_keywords": [],
         "packaging_options": ["Bulk", "1,000 kg Big Bags", "25/50 kg PP Bags"]},
    ],
    "incoterms":  ["FOB Santos", "FOB Paranaguá", "CIF ASWP"],
    "extra_rules": {},
}

# ─────────────────────────────────────────────────────────────────────────────
# Variáveis transacionais imutáveis (aplicáveis a todos os templates)
# ─────────────────────────────────────────────────────────────────────────────
PAYMENT_TERMS = {
    "SBLC (MT760)": (
        "SBLC (MT760), Irrevocable, Transferable, Cash Backed, "
        "Issued by a Prime Bank"
    ),
    "DLC (MT700)": (
        "DLC/LC (MT700) Irrevocable, Transferable, Cash Backed, "
        "Issued by a Prime Bank"
    ),
}

PERFORMANCE_BOND_TEXTS = {
    "12 months": (
        "Performance Bond: 2% PB to be issued by the Seller's bank against "
        "the Buyer's operative financial instrument."
    ),
    "Spot/Trial": "",
}


# ═════════════════════════════════════════════════════════════════════════════
# Registry e helpers
# ═════════════════════════════════════════════════════════════════════════════

COMMODITIES: Dict[str, dict] = {
    "SOY":     SOY,
    "CORN":    CORN,
    "SUGAR":   SUGAR,
    "CHICKEN": CHICKEN,
    "VEGOIL":  VEGOIL,
    "COTTON":  COTTON,
    "RICE":    RICE,
}


def get_commodity(code: str) -> dict:
    code = code.upper()
    if code not in COMMODITIES:
        raise KeyError(
            f"Commodity desconhecida: {code}. Disponíveis: {list(COMMODITIES)}"
        )
    return COMMODITIES[code]


def get_product(code: str, product_label: str) -> dict:
    com = get_commodity(code)
    for p in com["products"]:
        if p["label"] == product_label:
            return p
    available = [p["label"] for p in com["products"]]
    raise KeyError(
        f"Produto '{product_label}' não existe em {code}. Disponíveis: {available}"
    )


def list_product_labels(code: str) -> List[str]:
    return [p["label"] for p in get_commodity(code)["products"]]
