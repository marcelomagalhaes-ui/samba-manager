# -*- coding: utf-8 -*-
"""
dashboards/imfpa_widget.py
==========================
Página de confecção de IMFPA — Irrevocable Master Fee Protection Agreement.

Seções:
  A · Dados da Transação (commodity, quantidade, SPA code, taxas, data)
  B · Número de Intermediários (1, 2 ou 3)
  C · Dados das Partes (empresa + representante legal — por N)
  D · Dados Bancários (por N)
  Geração e download

CSS/Tema: idêntico ao loi_widget.py e ncnda_widget.py (light, #F4F5F7).
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path
from typing import Optional

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ─── CSS ─────────────────────────────────────────────────────────────────────

_FONT = (
    '<link href="https://fonts.googleapis.com/css2?family=Montserrat'
    ':wght@400;500;600;700;800;900&display=swap" rel="stylesheet">'
)

_STYLE = """<style>
/* ── reset ── */
.stApp                    { background:#F4F5F7 !important; font-family:'Montserrat',sans-serif !important; }
[data-testid="stSidebar"],
[data-testid="collapsedControl"] { display:none !important; }
header, footer            { display:none !important; }
.block-container          { padding:28px 40px 48px !important; max-width:1000px !important; margin:0 auto !important; }
div[data-testid="column"] { padding:0 6px !important; }

/* labels */
label, .stTextInput label, .stSelectbox label,
.stNumberInput label, .stTextArea label,
.stDateInput label, .stRadio label {
  font-family:'Montserrat',sans-serif !important;
  font-size:10px !important; font-weight:700 !important;
  letter-spacing:.8px !important; color:#1A1A1A !important; text-transform:uppercase;
}

/* inputs */
.stTextInput input, .stNumberInput input,
.stSelectbox [data-baseweb="select"] {
  font-family:'Montserrat',sans-serif !important;
  font-size:12px !important; border-radius:6px !important;
}

/* buttons */
div.stButton > button {
  background:transparent !important; color:#7F7F7F !important;
  border:1px solid #D9D9D9 !important; border-radius:6px !important;
  font-family:Montserrat,sans-serif !important; font-size:10px !important;
  font-weight:600 !important; letter-spacing:.8px !important; padding:7px 16px !important;
}
div.stButton > button:hover { border-color:#FA8200 !important; color:#FA8200 !important; }
div.stButton > button[kind="primary"] {
  background:#FA8200 !important; color:#fff !important;
  border:none !important; border-radius:6px !important;
  font-family:Montserrat,sans-serif !important; font-size:10px !important;
  font-weight:700 !important; letter-spacing:1px !important; padding:8px 20px !important;
}
div.stButton > button[kind="primary"]:hover { background:#C86600 !important; }

/* section cards */
.sec-card {
  background:#fff; border-radius:10px; padding:24px 28px 20px;
  border:1px solid #E8E9EC; margin-bottom:20px;
  box-shadow:0 1px 4px rgba(0,0,0,0.04);
}
.sec-label {
  font-size:9px; font-weight:700; letter-spacing:2.5px; color:#BFBFBF;
  font-family:Montserrat,sans-serif; margin-bottom:4px;
}
.sec-title {
  font-size:15px; font-weight:800; color:#1A1A1A;
  font-family:Montserrat,sans-serif; margin-bottom:14px;
}
.party-header {
  font-size:11px; font-weight:700; color:#FA8200;
  font-family:Montserrat,sans-serif; letter-spacing:1px;
  border-bottom:1px solid #F0F0F0; padding-bottom:6px; margin:16px 0 12px;
}
</style>"""


# ─── Helpers de UI ───────────────────────────────────────────────────────────

def _section(label: str, title: str) -> None:
    st.markdown(
        f'<div class="sec-label">{label}</div>'
        f'<div class="sec-title">{title}</div>',
        unsafe_allow_html=True,
    )


def _party_header(n: int, role: str = "INTERMEDIARY") -> None:
    st.markdown(
        f'<div class="party-header">PARTY {n} — {role}</div>',
        unsafe_allow_html=True,
    )


# ─── Widget principal ─────────────────────────────────────────────────────────

def render_imfpa_widget() -> None:
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
  <div style="font-size:11px;font-weight:700;letter-spacing:3px;color:#FA8200">IMFPA</div>
  <div style="font-size:10px;color:#9aa0a6;margin-top:8px;font-weight:500">
    Irrevocable Master Fee Protection Agreement &nbsp;·&nbsp; 1, 2 ou 3 intermediários
  </div>
</div>""", unsafe_allow_html=True)
    with hc3:
        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        if st.button("← Documentos", key="imfpa_back"):
            st.session_state.current_view = "documentos"
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # SEÇÃO A — Dados da Transação
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown('<div class="sec-card">', unsafe_allow_html=True)
    _section("SEÇÃO A", "Dados da Transação")

    ca1, ca2 = st.columns(2)
    with ca1:
        commodity = st.text_input(
            "COMMODITY",
            value=st.session_state.get("imfpa_commodity", "SOYBEAN MEAL"),
            placeholder="Ex: SOYBEAN MEAL, CORN, SUGAR",
            key="imfpa_commodity",
        )
        quantity_mt = st.text_input(
            "QUANTIDADE (MT)",
            value=st.session_state.get("imfpa_quantity_mt", ""),
            placeholder="Ex: 15,000",
            key="imfpa_quantity_mt",
        )
    with ca2:
        spa_code = st.text_input(
            "CÓDIGO DA SPA",
            value=st.session_state.get("imfpa_spa_code", ""),
            placeholder="Ex: SPA-2026-001",
            key="imfpa_spa_code",
        )
        date_val = st.date_input(
            "DATA DO DOCUMENTO",
            value=st.session_state.get("imfpa_date", datetime.date.today()),
            key="imfpa_date",
            format="DD/MM/YYYY",
        )

    ca3, ca4 = st.columns(2)
    with ca3:
        fee_per_shipment = st.text_input(
            "TAXA POR EMBARQUE (USD/MT)",
            value=st.session_state.get("imfpa_fee_per_shipment", ""),
            placeholder="Ex: 2.50",
            key="imfpa_fee_per_shipment",
        )
    with ca4:
        fee_total = st.text_input(
            "TAXA TOTAL (USD/MT)",
            value=st.session_state.get("imfpa_fee_total", ""),
            placeholder="Ex: 2.50",
            key="imfpa_fee_total",
        )

    st.markdown("</div>", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # SEÇÃO B — Número de Intermediários
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown('<div class="sec-card">', unsafe_allow_html=True)
    _section("SEÇÃO B", "Número de Intermediários")

    n_parties = st.radio(
        "Quantas partes intermediárias este IMFPA cobre?",
        options=[1, 2, 3],
        index=int(st.session_state.get("imfpa_n_parties", 1)) - 1,
        horizontal=True,
        key="imfpa_n_parties",
        help=(
            "1 = template 1IMFPA  |  "
            "2 = template 2IMFPA  |  "
            "3 = template 3IMFPA"
        ),
    )

    st.markdown("</div>", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # SEÇÕES C + D — Por parte N
    # ═══════════════════════════════════════════════════════════════════════
    for n in range(1, n_parties + 1):

        # ── Seção C — Empresa & Representante ────────────────────────────
        st.markdown('<div class="sec-card">', unsafe_allow_html=True)
        _section(f"SEÇÃO C · PARTE {n}", "Empresa & Representante Legal")
        _party_header(n)

        cc1, cc2 = st.columns(2)
        with cc1:
            st.text_input(
                "RAZÃO SOCIAL (Full Name)",
                value=st.session_state.get(f"imfpa_company_name_{n}", ""),
                placeholder="Ex: ACME TRADING LLC",
                key=f"imfpa_company_name_{n}",
            )
            st.text_input(
                "PAÍS",
                value=st.session_state.get(f"imfpa_country_{n}", ""),
                placeholder="Ex: United States",
                key=f"imfpa_country_{n}",
            )
            st.text_input(
                "TAX ID / CNPJ / VAT",
                value=st.session_state.get(f"imfpa_tax_id_{n}", ""),
                placeholder="Ex: 12-3456789",
                key=f"imfpa_tax_id_{n}",
            )
        with cc2:
            st.text_input(
                "ENDEREÇO REGISTRADO",
                value=st.session_state.get(f"imfpa_address_{n}", ""),
                placeholder="Ex: 123 Main St, New York, NY 10001",
                key=f"imfpa_address_{n}",
            )
            st.text_input(
                "NOME DO REPRESENTANTE LEGAL",
                value=st.session_state.get(f"imfpa_legal_rep_{n}", ""),
                placeholder="Ex: John Smith",
                key=f"imfpa_legal_rep_{n}",
            )
            st.text_input(
                "PASSAPORTE / ID DO REPRESENTANTE",
                value=st.session_state.get(f"imfpa_passport_{n}", ""),
                placeholder="Ex: AA1234567",
                key=f"imfpa_passport_{n}",
            )

        st.markdown("</div>", unsafe_allow_html=True)

        # ── Seção D — Dados Bancários ─────────────────────────────────────
        st.markdown('<div class="sec-card">', unsafe_allow_html=True)
        _section(f"SEÇÃO D · PARTE {n}", "Dados Bancários")

        cd1, cd2 = st.columns(2)
        with cd1:
            st.text_input(
                "NOME DO BENEFICIÁRIO",
                value=st.session_state.get(f"imfpa_beneficiary_{n}", ""),
                placeholder="Ex: ACME TRADING LLC",
                key=f"imfpa_beneficiary_{n}",
            )
            st.text_input(
                "NÚMERO DO DOCUMENTO / REFERÊNCIA",
                value=st.session_state.get(f"imfpa_doc_number_{n}", ""),
                placeholder="Ex: ACC-2026-001",
                key=f"imfpa_doc_number_{n}",
            )
            st.text_input(
                "NOME DO BANCO",
                value=st.session_state.get(f"imfpa_bank_name_{n}", ""),
                placeholder="Ex: JPMorgan Chase Bank",
                key=f"imfpa_bank_name_{n}",
            )
        with cd2:
            st.text_input(
                "SWIFT / BIC",
                value=st.session_state.get(f"imfpa_swift_{n}", ""),
                placeholder="Ex: CHASUS33",
                key=f"imfpa_swift_{n}",
            )
            st.text_input(
                "IBAN / ACCOUNT NUMBER",
                value=st.session_state.get(f"imfpa_iban_{n}", ""),
                placeholder="Ex: US12345678901234567890",
                key=f"imfpa_iban_{n}",
            )

        st.markdown("</div>", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # Botão de geração
    # ═══════════════════════════════════════════════════════════════════════
    st.markdown('<div class="sec-card">', unsafe_allow_html=True)
    _section("GERAÇÃO", "Gerar IMFPA")

    out_fmt = st.radio(
        "Formato de saída",
        options=["pdf", "gdoc"],
        index=0,
        horizontal=True,
        format_func=lambda x: "PDF (recomendado)" if x == "pdf" else "Google Doc",
        key="imfpa_output_format",
    )

    col_btn, col_dry = st.columns([3, 1])
    with col_btn:
        gerar = st.button("Gerar IMFPA →", type="primary", key="imfpa_gerar", use_container_width=True)
    with col_dry:
        dry = st.button("Dry-run", key="imfpa_dry", use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # ═══════════════════════════════════════════════════════════════════════
    # Execução
    # ═══════════════════════════════════════════════════════════════════════
    if gerar or dry:
        # ── Montar payload ────────────────────────────────────────────────
        date_obj = st.session_state.get("imfpa_date", datetime.date.today())
        if isinstance(date_obj, datetime.date):
            date_str = date_obj.strftime("%d/%m/%Y")
            # doc_code: DDMMYY-FIRSTNAMECOMPANY1
            company_1 = st.session_state.get("imfpa_company_name_1", "SAMBA")
            first_word = (company_1.split()[0] if company_1 else "SAMBA")
            doc_code = date_obj.strftime("%d%m%y") + "-" + first_word.upper()
        else:
            date_str = str(date_obj)
            doc_code = ""

        user_inputs = {
            "DATE":             date_str,
            "DOC_CODE":         doc_code,
            "QUANTITY_MT":      st.session_state.get("imfpa_quantity_mt", ""),
            "SPA_CODE":         st.session_state.get("imfpa_spa_code", ""),
            "COMMODITY":        (st.session_state.get("imfpa_commodity", "") or "").upper(),
            "FEE_PER_SHIPMENT": st.session_state.get("imfpa_fee_per_shipment", ""),
            "FEE_TOTAL":        st.session_state.get("imfpa_fee_total", ""),
        }

        for n in range(1, n_parties + 1):
            user_inputs[f"COMPANY_NAME_{n}"]     = st.session_state.get(f"imfpa_company_name_{n}", "")
            user_inputs[f"COUNTRY_{n}"]          = st.session_state.get(f"imfpa_country_{n}", "")
            user_inputs[f"TAX_ID_{n}"]           = st.session_state.get(f"imfpa_tax_id_{n}", "")
            user_inputs[f"ADDRESS_{n}"]          = st.session_state.get(f"imfpa_address_{n}", "")
            user_inputs[f"LEGAL_REP_NAME_{n}"]   = st.session_state.get(f"imfpa_legal_rep_{n}", "")
            user_inputs[f"PASSPORT_{n}"]         = st.session_state.get(f"imfpa_passport_{n}", "")
            user_inputs[f"BENEFICIARY_NAME_{n}"] = st.session_state.get(f"imfpa_beneficiary_{n}", "")
            user_inputs[f"DOC_NUMBER_{n}"]       = st.session_state.get(f"imfpa_doc_number_{n}", "")
            user_inputs[f"BANK_NAME_{n}"]        = st.session_state.get(f"imfpa_bank_name_{n}", "")
            user_inputs[f"SWIFT_{n}"]            = st.session_state.get(f"imfpa_swift_{n}", "")
            user_inputs[f"IBAN_{n}"]             = st.session_state.get(f"imfpa_iban_{n}", "")

        payload = {
            "n_parties":     n_parties,
            "user_inputs":   user_inputs,
            "output_format": out_fmt,
            "dry_run":       dry and not gerar,
        }

        # ── Chamar agente ─────────────────────────────────────────────────
        with st.spinner("Gerando IMFPA…"):
            try:
                from agents.imfpa_generator_agent import IMFPAGeneratorAgent
                agent  = IMFPAGeneratorAgent()
                result = agent.process(payload)
            except Exception as exc:
                result = {"status": "error", "error": str(exc)}

        # ── Exibir resultado ──────────────────────────────────────────────
        if result.get("status") == "success":
            if result.get("dry_run"):
                st.success("✅ Dry-run concluído — nenhum arquivo foi enviado ao Drive.")
                st.json({k: v for k, v in result.items() if k != "status"})
            else:
                st.success(f"✅ IMFPA gerado com sucesso: **{result.get('filename')}**")
                web_link = result.get("web_link")
                if web_link:
                    st.markdown(
                        f'<a href="{web_link}" target="_blank" style="font-family:Montserrat;'
                        f'font-size:12px;color:#FA8200;font-weight:700">Abrir no Drive →</a>',
                        unsafe_allow_html=True,
                    )

                # Download direto se bytes disponíveis (PDF)
                pdf_bytes = result.get("file_bytes")
                if pdf_bytes:
                    st.download_button(
                        label="⬇ Baixar PDF",
                        data=pdf_bytes,
                        file_name=result.get("filename", "IMFPA.pdf"),
                        mime="application/pdf",
                        key="imfpa_download",
                    )

                # Alertas
                for alert in result.get("alerts", []):
                    st.warning(f"⚠ {alert}")

        else:
            st.error(f"❌ {result.get('error', 'Erro desconhecido.')}")

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown("""
<div style="text-align:center;font-family:Montserrat,sans-serif;font-size:9px;
  color:#D9D9D9;letter-spacing:1px;padding:28px 0 16px">
  SAMBA EXPORT &nbsp;·&nbsp; IMFPA GENERATOR &nbsp;·&nbsp; USO INTERNO RESTRITO
</div>""", unsafe_allow_html=True)
