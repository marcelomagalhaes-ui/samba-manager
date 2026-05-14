# -*- coding: utf-8 -*-
"""
dashboards/imfpa_widget.py
==========================
Página de confecção de IMFPA — Irrevocable Master Fee Protection Agreement.

Seções:
  A · Seleção de Commodity e Quantidade
  B · Número de Intermediários (1, 2 ou 3)
  C · Dados das Partes (empresa + representante legal — por N)
  D · Dados Bancários (por N)

CSS/Tema: idêntico ao loi_widget.py e ncnda_widget.py (light, #F4F5F7).
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path

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
div[data-testid="column"] { padding:0 8px !important; }

/* ── labels ── */
label, .stRadio label, .stCheckbox label,
.stSelectbox label, .stTextInput label,
.stNumberInput label, .stTextArea label,
.stDateInput label {
    font-family:'Montserrat',sans-serif !important;
    font-size:9px !important; font-weight:700 !important;
    letter-spacing:1.2px !important; color:#7f7f7f !important;
    text-transform:uppercase !important; }

/* ── inputs / textarea ── */
.stTextInput input, .stNumberInput input {
    background:#fff !important; color:#262626 !important;
    border:1px solid #E0E0E0 !important; border-radius:7px !important;
    font-family:'Montserrat',sans-serif !important; font-size:13px !important; }
.stTextInput input:focus, .stNumberInput input:focus {
    border-color:#FA8200 !important;
    box-shadow:0 0 0 2px rgba(250,130,0,.14) !important; }
.stTextArea textarea {
    background:#fff !important; color:#262626 !important;
    border:1px solid #E0E0E0 !important; border-radius:7px !important;
    font-family:'Montserrat',sans-serif !important; font-size:13px !important;
    resize:vertical !important; }
.stTextArea textarea:focus {
    border-color:#FA8200 !important;
    box-shadow:0 0 0 2px rgba(250,130,0,.14) !important; }

/* ── selectbox ── */
div[data-baseweb="select"] > div {
    background:#fff !important; border:1px solid #E0E0E0 !important;
    border-radius:7px !important; color:#262626 !important;
    font-family:'Montserrat',sans-serif !important; }

/* ── radio ── */
.stRadio > div    { gap:10px !important; }
.stRadio > div label { font-size:11px !important; font-weight:600 !important;
    letter-spacing:.5px !important; text-transform:none !important;
    color:#7f7f7f !important; }

/* ── botões base ── */
div.stButton > button {
    background:transparent !important; color:#7F7F7F !important;
    border:1px solid #D9D9D9 !important; border-radius:7px !important;
    font-family:'Montserrat',sans-serif !important; font-size:10px !important;
    font-weight:600 !important; letter-spacing:.8px !important; }
div.stButton > button:hover {
    border-color:#FA8200 !important; color:#FA8200 !important; }

/* ── botão primário (Gerar IMFPA) ── */
div.stButton > button[kind="primaryFormSubmit"],
div.stButton > button[kind="primary"] {
    background:#FA8200 !important; color:#fff !important;
    border:none !important; border-radius:8px !important;
    font-family:'Montserrat',sans-serif !important;
    font-size:12px !important; font-weight:700 !important;
    letter-spacing:1px !important; padding:11px 0 !important; }
div.stButton > button[kind="primary"]:hover { background:#C86600 !important; }

/* ── divider ── */
[data-testid="stDivider"] { border-color:#E8E9EC !important; }

/* ── expander ── */
[data-testid="stExpander"] {
    border:1px solid #E8E9EC !important; border-radius:8px !important;
    background:#fff !important; }
[data-testid="stExpander"] summary {
    font-family:'Montserrat',sans-serif !important;
    font-size:11px !important; color:#7f7f7f !important; }

/* ── caption / alert ── */
[data-testid="stCaptionContainer"] {
    color:#7f7f7f !important; font-family:'Montserrat',sans-serif !important;
    font-size:10px !important; }
[data-testid="stAlertContainer"] {
    font-family:'Montserrat',sans-serif !important; font-size:11px !important; }
</style>"""


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _sec(label: str, title: str) -> None:
    st.markdown(
        f'<div style="margin:28px 0 14px">'
        f'<span style="font-size:9px;font-weight:700;letter-spacing:2.5px;'
        f'color:#FA8200;font-family:Montserrat,sans-serif;text-transform:uppercase">'
        f'{label}</span>&nbsp;&nbsp;'
        f'<span style="font-size:15px;font-weight:800;color:#1A1A1A;'
        f'font-family:Montserrat,sans-serif;letter-spacing:-.2px">{title}</span></div>',
        unsafe_allow_html=True,
    )


def _helper(text: str) -> None:
    st.markdown(
        f'<div style="font-size:10px;color:#555;font-style:italic;'
        f'margin-top:-8px;margin-bottom:8px;font-family:Montserrat,sans-serif">'
        f'{text}</div>',
        unsafe_allow_html=True,
    )


def _party_card_header(n: int, label: str = "INTERMEDIARY") -> None:
    st.markdown(
        f'<div style="margin:20px 0 14px;padding:14px 20px;background:#fff;'
        f'border:1px solid #E0E0E0;border-radius:10px;border-left:4px solid #FA8200;'
        f'font-family:Montserrat,sans-serif">'
        f'<span style="font-size:9px;font-weight:700;letter-spacing:2.5px;color:#FA8200">'
        f'PARTE {n}</span>'
        f'<span style="font-size:14px;font-weight:800;color:#1A1A1A;margin-left:14px">'
        f'{label}</span></div>',
        unsafe_allow_html=True,
    )


# ─── Render principal ─────────────────────────────────────────────────────────

def render_imfpa_widget() -> None:
    """Página completa de confecção de IMFPA (light theme, padrão LOI/NCNDA)."""

    if "imfpa_dry_run" not in st.session_state:
        st.session_state["imfpa_dry_run"] = False

    st.markdown(_FONT,  unsafe_allow_html=True)
    st.markdown(_STYLE, unsafe_allow_html=True)

    logo_path = ROOT / "assets" / "logo.png"

    # ── Header ────────────────────────────────────────────────────────────────
    hc1, hc2, hc3 = st.columns([0.18, 0.62, 0.20])
    with hc1:
        if logo_path.exists():
            st.markdown("<div style='padding:10px 0 0'></div>", unsafe_allow_html=True)
            st.image(str(logo_path), width=180)
    with hc2:
        st.markdown(
            '<div style="padding:14px 0 8px;font-family:Montserrat,sans-serif">'
            '<div style="font-size:10px;font-weight:700;letter-spacing:3px;color:#FA8200">'
            'GERADOR DE DOCUMENTOS &nbsp;·&nbsp; IMFPA</div>'
            '<div style="font-size:11px;color:#555;margin-top:6px;font-weight:500">'
            'Irrevocable Master Fee Protection Agreement'
            ' &nbsp;·&nbsp; 1, 2 ou 3 intermediários &nbsp;·&nbsp; Output PDF</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with hc3:
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        if st.button("← Documentos", key="imfpa_back"):
            st.session_state.current_view = "documentos"
            st.rerun()

    st.markdown(
        '<div style="height:1px;background:#E8E9EC;margin:0 0 4px"></div>',
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO A · COMMODITY E TRANSAÇÃO
    # ══════════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO A", "Commodity e Transação")

    ac1, ac2 = st.columns(2)
    with ac1:
        commodity = st.text_input(
            "Commodity *",
            key="imfpa_commodity",
            placeholder="Ex.: SOYBEAN MEAL, CORN, SUGAR",
            help="Substitui o literal SOYBEAN no template.",
        )
        quantity_mt = st.text_input(
            "Quantidade (MT) *",
            key="imfpa_quantity_mt",
            placeholder="Ex.: 15,000",
        )
        spa_code = st.text_input(
            "Código da SPA",
            key="imfpa_spa_code",
            placeholder="Ex.: SPA-2026-001",
        )
    with ac2:
        date_val = st.date_input(
            "Data do Documento",
            value=st.session_state.get("imfpa_date_val", datetime.date.today()),
            key="imfpa_date_val",
            format="DD/MM/YYYY",
        )
        fee_per_shipment = st.text_input(
            "Taxa por Embarque (USD/MT) *",
            key="imfpa_fee_per_shipment",
            placeholder="Ex.: 2.50",
            help="Primeira ocorrência de 'USD X.XX per MT' no template.",
        )
        fee_total = st.text_input(
            "Taxa Total (USD/MT) *",
            key="imfpa_fee_total",
            placeholder="Ex.: 2.50",
            help="Segunda ocorrência de 'USD X.XX per MT' no template.",
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO B · NÚMERO DE INTERMEDIÁRIOS
    # ══════════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO B", "Número de Intermediários")

    _helper(
        "1 intermediário → template 1IMFPA  |  "
        "2 → 2IMFPA  |  3 → 3IMFPA"
    )

    n_parties = st.radio(
        "Quantas partes intermediárias este IMFPA cobre?",
        options=[1, 2, 3],
        index=0,
        horizontal=True,
        key="imfpa_n_parties",
    )

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÕES C + D · POR PARTE N
    # ══════════════════════════════════════════════════════════════════════════
    for n in range(1, n_parties + 1):

        # ── Seção C — Empresa & Representante ────────────────────────────────
        _sec(f"SEÇÃO C · PARTE {n}", "Empresa & Representante Legal")
        _party_card_header(n)

        cc1, cc2 = st.columns(2)
        with cc1:
            st.text_input(
                f"Razão Social *  (Full Name Company {n})",
                key=f"imfpa_company_name_{n}",
                placeholder="Ex.: ACME TRADING LLC",
            )
            st.text_input(
                f"País *  (Country {n})",
                key=f"imfpa_country_{n}",
                placeholder="Ex.: United States",
            )
            st.text_input(
                f"Tax ID / CNPJ / VAT *",
                key=f"imfpa_tax_id_{n}",
                placeholder="Ex.: 12-3456789",
            )
        with cc2:
            st.text_area(
                f"Endereço Registrado *",
                key=f"imfpa_address_{n}",
                placeholder="Ex.: 123 Main St, New York, NY 10001, USA",
                height=80,
            )
            st.text_input(
                f"Nome do Representante Legal *",
                key=f"imfpa_legal_rep_{n}",
                placeholder="Ex.: John Smith",
            )
            st.text_input(
                f"Passaporte / ID do Representante *",
                key=f"imfpa_passport_{n}",
                placeholder="Ex.: AA1234567",
            )

        # ── Seção D — Dados Bancários ─────────────────────────────────────────
        _sec(f"SEÇÃO D · PARTE {n}", "Dados Bancários")

        cd1, cd2 = st.columns(2)
        with cd1:
            st.text_input(
                f"Nome do Beneficiário *",
                key=f"imfpa_beneficiary_{n}",
                placeholder="Ex.: ACME TRADING LLC",
            )
            st.text_input(
                f"Número do Documento / Referência *",
                key=f"imfpa_doc_number_{n}",
                placeholder="Ex.: ACC-2026-001",
            )
            st.text_input(
                f"Nome do Banco *",
                key=f"imfpa_bank_name_{n}",
                placeholder="Ex.: JPMorgan Chase Bank",
            )
        with cd2:
            st.text_input(
                f"SWIFT / BIC *",
                key=f"imfpa_swift_{n}",
                placeholder="Ex.: CHASUS33",
            )
            st.text_input(
                f"IBAN / Account Number *",
                key=f"imfpa_iban_{n}",
                placeholder="Ex.: US12345678901234567890",
            )

    st.divider()

    # ══════════════════════════════════════════════════════════════════════════
    # AÇÃO FINAL
    # ══════════════════════════════════════════════════════════════════════════
    with st.expander("⚙ Opções avançadas", expanded=False):
        dry_run = st.checkbox(
            "Dry-run — validar sem upload (não sobe ao Drive)",
            value=False,
            key="imfpa_dry_run",
        )
        if dry_run:
            st.warning("Modo dry-run ativo — nenhum arquivo será enviado ao Drive.")

    dry_run = st.session_state.get("imfpa_dry_run", False)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    submit = st.button(
        "Gerar IMFPA →",
        key="imfpa_submit",
        type="primary",
        use_container_width=True,
    )

    if not submit:
        return

    # ── Validações ────────────────────────────────────────────────────────────
    errors: list[str] = []

    if not (st.session_state.get("imfpa_commodity") or "").strip():
        errors.append("Commodity é obrigatória (Seção A).")
    if not (st.session_state.get("imfpa_quantity_mt") or "").strip():
        errors.append("Quantidade (MT) é obrigatória (Seção A).")
    if not (st.session_state.get("imfpa_fee_per_shipment") or "").strip():
        errors.append("Taxa por Embarque é obrigatória (Seção A).")
    if not (st.session_state.get("imfpa_fee_total") or "").strip():
        errors.append("Taxa Total é obrigatória (Seção A).")

    for n in range(1, n_parties + 1):
        required = {
            f"Razão Social (Parte {n})":           f"imfpa_company_name_{n}",
            f"País (Parte {n})":                   f"imfpa_country_{n}",
            f"Tax ID (Parte {n})":                 f"imfpa_tax_id_{n}",
            f"Endereço (Parte {n})":               f"imfpa_address_{n}",
            f"Representante Legal (Parte {n})":    f"imfpa_legal_rep_{n}",
            f"Passaporte (Parte {n})":             f"imfpa_passport_{n}",
            f"Beneficiário (Parte {n})":           f"imfpa_beneficiary_{n}",
            f"Número do Documento (Parte {n})":    f"imfpa_doc_number_{n}",
            f"Nome do Banco (Parte {n})":          f"imfpa_bank_name_{n}",
            f"SWIFT (Parte {n})":                  f"imfpa_swift_{n}",
            f"IBAN (Parte {n})":                   f"imfpa_iban_{n}",
        }
        for label, key in required.items():
            if not (st.session_state.get(key) or "").strip():
                errors.append(f"{label} é obrigatório.")

    if errors:
        for e in errors:
            st.error(e)
        return

    # ── Monta payload ──────────────────────────────────────────────────────────
    date_obj = st.session_state.get("imfpa_date_val", datetime.date.today())
    date_str = date_obj.strftime("%d/%m/%Y") if isinstance(date_obj, datetime.date) else str(date_obj)

    company_1  = (st.session_state.get("imfpa_company_name_1") or "SAMBA").strip()
    first_word = company_1.split()[0].upper()
    doc_code   = (date_obj.strftime("%d%m%y") if isinstance(date_obj, datetime.date) else "") + f"-{first_word}"

    user_inputs: dict = {
        "DATE":             date_str,
        "DOC_CODE":         doc_code,
        "QUANTITY_MT":      (st.session_state.get("imfpa_quantity_mt") or "").strip(),
        "SPA_CODE":         (st.session_state.get("imfpa_spa_code") or "").strip(),
        "COMMODITY":        (st.session_state.get("imfpa_commodity") or "").strip().upper(),
        "FEE_PER_SHIPMENT": (st.session_state.get("imfpa_fee_per_shipment") or "").strip(),
        "FEE_TOTAL":        (st.session_state.get("imfpa_fee_total") or "").strip(),
    }

    for n in range(1, n_parties + 1):
        user_inputs[f"COMPANY_NAME_{n}"]     = (st.session_state.get(f"imfpa_company_name_{n}") or "").strip()
        user_inputs[f"COUNTRY_{n}"]          = (st.session_state.get(f"imfpa_country_{n}") or "").strip()
        user_inputs[f"TAX_ID_{n}"]           = (st.session_state.get(f"imfpa_tax_id_{n}") or "").strip()
        user_inputs[f"ADDRESS_{n}"]          = (st.session_state.get(f"imfpa_address_{n}") or "").strip()
        user_inputs[f"LEGAL_REP_NAME_{n}"]   = (st.session_state.get(f"imfpa_legal_rep_{n}") or "").strip()
        user_inputs[f"PASSPORT_{n}"]         = (st.session_state.get(f"imfpa_passport_{n}") or "").strip()
        user_inputs[f"BENEFICIARY_NAME_{n}"] = (st.session_state.get(f"imfpa_beneficiary_{n}") or "").strip()
        user_inputs[f"DOC_NUMBER_{n}"]       = (st.session_state.get(f"imfpa_doc_number_{n}") or "").strip()
        user_inputs[f"BANK_NAME_{n}"]        = (st.session_state.get(f"imfpa_bank_name_{n}") or "").strip()
        user_inputs[f"SWIFT_{n}"]            = (st.session_state.get(f"imfpa_swift_{n}") or "").strip()
        user_inputs[f"IBAN_{n}"]             = (st.session_state.get(f"imfpa_iban_{n}") or "").strip()

    payload = {
        "n_parties":     n_parties,
        "user_inputs":   user_inputs,
        "output_format": "pdf",
        "dry_run":       dry_run,
    }

    # ── Chama o agente ─────────────────────────────────────────────────────────
    from agents.imfpa_generator_agent import IMFPAGeneratorAgent

    agent = IMFPAGeneratorAgent()
    with st.spinner(f"Gerando IMFPA ({n_parties} parte{'s' if n_parties > 1 else ''})…"):
        try:
            res = agent.process(payload)
        except Exception as exc:
            res = {"status": "error", "error": str(exc)}

    # ── Resultado ──────────────────────────────────────────────────────────────
    ok         = res.get("status") == "success"
    border_col = "#329632" if ok else "#FA3232"
    icon       = "✓" if ok else "✗"

    if ok and not dry_run:
        st.markdown(
            '<div style="background:#f0faf0;border:1px solid #329632;border-radius:10px;'
            'padding:18px 22px;margin:18px 0 12px;font-family:Montserrat,sans-serif">'
            '<div style="font-size:13px;font-weight:800;color:#2a7a2a;margin-bottom:4px">'
            '✓ IMFPA gerado com sucesso</div>'
            '<div style="font-size:10px;color:#7f7f7f">Arquivo salvo no Google Drive — '
            'clique abaixo para abrir ou baixar</div></div>',
            unsafe_allow_html=True,
        )

    if ok and dry_run:
        body = (
            f"<strong style='color:#1A1A1A'>Dry-run OK</strong> &nbsp;·&nbsp; "
            f"{res.get('filename', '')}"
            f"<div style='font-size:10px;color:#7f7f7f;margin-top:4px'>"
            f"{res.get('size_bytes', 0):,} bytes</div>"
        )
    elif ok:
        link = res.get("web_link", "")
        body = (
            f"<div style='font-size:13px;font-weight:700;color:#1A1A1A;margin-bottom:8px'>"
            f"{res.get('filename', '')}</div>"
            + (
                f'<a href="{link}" target="_blank" '
                f'style="background:#FA8200;color:#fff;font-size:11px;font-weight:700;'
                f'text-decoration:none;padding:7px 16px;border-radius:5px;'
                f'display:inline-block;margin-right:8px">'
                f'Abrir no Google Drive →</a>'
                if link else ""
            )
            + f"<div style='font-size:10px;color:#7f7f7f;margin-top:8px'>"
            f"{res.get('size_bytes', 0):,} bytes &nbsp;·&nbsp; formato PDF</div>"
        )
    else:
        err = res.get("error", "erro desconhecido")
        body = (
            f"<div style='font-size:12px;font-weight:700;color:#FA3232;margin-bottom:4px'>"
            f"Falha na geração</div>"
            f"<div style='font-size:11px;color:#262626;word-break:break-all'>{err}</div>"
        )

    tpl = res.get("template_used", "")
    tpl_badge = f" &nbsp;·&nbsp; {tpl}" if tpl else ""
    st.markdown(
        f'<div style="background:#fff;border-radius:9px;padding:18px 20px;'
        f'border:1px solid #E8E9EC;border-left:4px solid {border_col};'
        f'margin-bottom:6px;font-family:Montserrat,sans-serif">'
        f'<div style="font-size:9px;font-weight:700;letter-spacing:1.5px;'
        f'color:#BFBFBF;margin-bottom:10px;text-transform:uppercase">'
        f'<span style="color:{border_col}">{icon}</span> &nbsp;'
        f'IMFPA &nbsp;·&nbsp; {n_parties} parte{"s" if n_parties > 1 else ""}{tpl_badge}</div>'
        f'{body}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Botão de download local
    if ok and not dry_run and res.get("file_bytes"):
        st.download_button(
            label="⬇  Baixar PDF",
            data=res["file_bytes"],
            file_name=res.get("filename", "IMFPA-SAMBA.pdf"),
            mime="application/pdf",
            key="imfpa_download",
        )

    for alert in res.get("alerts", []):
        st.warning(alert)

    if ok and dry_run:
        with st.expander("Detalhes dry-run · IMFPA"):
            st.markdown("**Substituições mapeadas:**")
            st.json(res.get("replacements", {}))
