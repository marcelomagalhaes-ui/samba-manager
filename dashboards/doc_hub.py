# -*- coding: utf-8 -*-
"""
dashboards/doc_hub.py
=====================
Gerador de Documentos — página de hub com grid de 12 tipos documentais.
Brand Manual v3: Montserrat · #FA8200 · sem gradientes · tema portal claro.
"""
from __future__ import annotations
import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ─── Catálogo completo de documentos ─────────────────────────────────────────
DOC_CATALOG = [
    {
        "code": "LOI",
        "name": "Letter of Intent",
        "desc": "Intenção formal de compra de commodities — 7 produtos disponíveis",
        "active": True,
        "wizard_key": "loi",
    },
    {
        "code": "NCNDA",
        "name": "NCNDA",
        "desc": "Non-Circumvention · Non-Disclosure Agreement",
        "active": True,
        "wizard_key": "ncnda",
    },
    {
        "code": "IMFPA",
        "name": "IMFPA",
        "desc": "Irrevocable Master Fee Protection Agreement",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "MOU",
        "name": "Memorandum of Understanding",
        "desc": "Acordo de intenções entre as partes",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "SCO",
        "name": "Soft Corporate Offer",
        "desc": "Oferta indicativa sem compromisso imediato",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "ICPO",
        "name": "ICPO",
        "desc": "Irrevocable Corporate Purchase Order",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "FCO",
        "name": "Full Corporate Offer",
        "desc": "Oferta corporativa completa e vinculante",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "SPA",
        "name": "Sales & Purchase Agreement",
        "desc": "Contrato de compra e venda definitivo",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "FC",
        "name": "Firme de Compra",
        "desc": "Confirmação de intenção firme de compra",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "PC",
        "name": "Proposta de Compra",
        "desc": "Proposta formal de compra ao vendedor",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "OV",
        "name": "Oferta de Venda",
        "desc": "Oferta pública ou privada de venda de commodity",
        "active": False,
        "wizard_key": "",
    },
    {
        "code": "CTV",
        "name": "Compra e Venda a Termo",
        "desc": "Contrato a termo com entrega futura de commodity",
        "active": False,
        "wizard_key": "",
    },
]

# ─── SVG icons ───────────────────────────────────────────────────────────────
_ICONS: dict[str, str] = {
    "LOI": (
        '<svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" '
        'stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">'
        '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>'
        '<polyline points="14 2 14 8 20 8"/>'
        '<line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>'
        '<polyline points="10 9 9 9 8 9"/></svg>'
    ),
    "NCNDA": (
        '<svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" '
        'stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">'
        '<rect x="3" y="11" width="18" height="11" rx="2"/>'
        '<path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>'
    ),
    "MOU": (
        '<svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" '
        'stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">'
        '<path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>'
        '<circle cx="9" cy="7" r="4"/>'
        '<path d="M23 21v-2a4 4 0 0 0-3-3.87"/>'
        '<path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>'
    ),
    "_default": (
        '<svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" '
        'stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">'
        '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>'
        '<polyline points="14 2 14 8 20 8"/></svg>'
    ),
}

_FONT = '<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">'

_STYLE = """<style>
.stApp { background: #F4F5F7 !important; font-family: 'Montserrat', sans-serif !important; }
[data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none !important; }
header { display: none !important; }
.block-container { padding: 24px 40px 48px !important; max-width: 1200px !important; margin: 0 auto !important; }
div[data-testid="column"] { padding: 0 8px !important; }

/* Back / nav buttons */
div.stButton > button {
    background: transparent !important; color: #7F7F7F !important;
    border: 1px solid #D9D9D9 !important; border-radius: 6px !important;
    font-family: Montserrat, sans-serif !important; font-size: 10px !important;
    font-weight: 600 !important; letter-spacing: .8px !important; padding: 7px 16px !important;
}
div.stButton > button:hover { border-color: #FA8200 !important; color: #FA8200 !important; }
div.stButton > button:disabled { color: #BFBFBF !important; border-color: #EBEBEB !important; cursor: default !important; }

/* Orange primary button */
div.stButton > button[kind="primary"] {
    background: #FA8200 !important; color: #fff !important;
    border: none !important; border-radius: 6px !important;
    font-family: Montserrat, sans-serif !important; font-size: 10px !important;
    font-weight: 700 !important; letter-spacing: 1px !important;
    padding: 8px 16px !important; width: 100% !important;
}
div.stButton > button[kind="primary"]:hover { background: #C86600 !important; }
</style>"""


def render_doc_hub() -> None:
    """Render the Document Generator hub (light portal theme)."""

    # Memoriza a view de origem ANTES de entrar no hub — não pode ser "documentos",
    # "loi" ou "ncnda" porque essas são views internas do próprio fluxo.
    _internal = {"documentos", "loi", "ncnda"}
    incoming = st.session_state.get("prev_view", "portal")
    if incoming not in _internal:
        st.session_state["_doc_hub_origin"] = incoming

    st.markdown(_FONT,  unsafe_allow_html=True)
    st.markdown(_STYLE, unsafe_allow_html=True)

    logo_path = ROOT / "assets" / "logo.png"

    # ── Header bar ────────────────────────────────────────────────────────────
    st.markdown('<div style="background:#fff;border-bottom:1px solid #E8E9EC;padding:0 32px">', unsafe_allow_html=True)

    hc1, hc2, hc3 = st.columns([0.18, 0.62, 0.20])
    with hc1:
        if logo_path.exists():
            st.image(str(logo_path), width=160)
    with hc2:
        st.markdown("""
<div style="font-family:Montserrat,sans-serif;padding:18px 0 10px">
  <div style="font-size:11px;font-weight:700;letter-spacing:3px;color:#FA8200">GERADOR DE DOCUMENTOS</div>
  <div style="font-size:10px;color:#9aa0a6;margin-top:8px;font-weight:500">
    Document Factory &nbsp;·&nbsp; Confecção automatizada de documentos comerciais
  </div>
</div>""", unsafe_allow_html=True)
    with hc3:
        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        origin = st.session_state.get("_doc_hub_origin", "portal")
        back_label = "← Portal" if origin == "portal" else "← Control Desk"
        if st.button(back_label, key="dochub_back"):
            st.session_state.current_view = origin
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Page content ──────────────────────────────────────────────────────────
    st.markdown('<div style="padding:32px 32px 0">', unsafe_allow_html=True)

    st.markdown("""
<div style="margin-bottom:32px">
  <div style="font-size:9px;font-weight:700;letter-spacing:2.5px;color:#BFBFBF;
    font-family:Montserrat,sans-serif;margin-bottom:6px">CICLO DOCUMENTAL COMERCIAL</div>
  <div style="font-size:22px;font-weight:800;color:#1A1A1A;font-family:Montserrat,sans-serif;
    letter-spacing:-.3px">Selecione o documento</div>
  <div style="font-size:11px;color:#7F7F7F;font-family:Montserrat,sans-serif;margin-top:6px;line-height:1.55">
    Templates base gerenciados no Drive · Preenchimento automático por IA ·
    Output em PDF · Nomenclatura padronizada
  </div>
</div>""", unsafe_allow_html=True)

    # ── Document grid: 4 cards per row ────────────────────────────────────────
    for row_start in range(0, len(DOC_CATALOG), 4):
        row_docs = DOC_CATALOG[row_start : row_start + 4]
        cols = st.columns(4, gap="medium")
        for col, doc in zip(cols, row_docs):
            active  = doc["active"]
            accent  = "#FA8200" if active else "#D9D9D9"
            icon    = _ICONS.get(doc["code"], _ICONS["_default"])
            opacity = "1" if active else "0.6"
            status  = (
                '<span style="font-size:9px;font-weight:700;letter-spacing:.3px;color:#329632">&#9679; Disponível</span>'
                if active else
                '<span style="font-size:9px;color:#BFBFBF;font-weight:500">Em breve</span>'
            )
            with col:
                st.markdown(f"""
<div style="background:#fff;border-radius:10px;padding:20px 20px 16px;
  border:1px solid #E8E9EC;border-top:3px solid {accent};
  box-shadow:0 1px 6px rgba(0,0,0,0.05);opacity:{opacity};
  font-family:Montserrat,sans-serif;min-height:170px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
    <div style="width:40px;height:40px;border-radius:8px;background:{accent}18;
      display:flex;align-items:center;justify-content:center;color:{accent};flex-shrink:0">
      {icon}
    </div>
    {status}
  </div>
  <div style="font-size:9px;font-weight:700;letter-spacing:2px;color:{accent};margin-bottom:4px">{doc['code']}</div>
  <div style="font-size:13px;font-weight:800;color:#1A1A1A;margin-bottom:8px;line-height:1.25">{doc['name']}</div>
  <div style="font-size:10px;color:#7F7F7F;line-height:1.5">{doc['desc']}</div>
</div>""", unsafe_allow_html=True)

                st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

                btn_key = f"dochub_open_{doc['code']}"
                if active and doc["wizard_key"]:
                    if st.button(f"Confeccionar {doc['code']} →", key=btn_key, type="primary", use_container_width=True):
                        # prev_view = "documentos" para que o widget saiba voltar pro hub
                        st.session_state.prev_view = "documentos"
                        st.session_state.current_view = doc["wizard_key"]
                        st.rerun()
                else:
                    st.button("Em desenvolvimento", key=btn_key, disabled=True, use_container_width=True)

        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("""
<div style="text-align:center;font-family:Montserrat,sans-serif;font-size:9px;
  color:#D9D9D9;letter-spacing:1px;padding:28px 0 16px">
  SAMBA EXPORT &nbsp;·&nbsp; DOCUMENT FACTORY &nbsp;·&nbsp; USO INTERNO RESTRITO
</div>""", unsafe_allow_html=True)
