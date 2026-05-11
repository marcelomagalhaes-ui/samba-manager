# -*- coding: utf-8 -*-
"""
dashboards/loi_widget.py
========================
Página de confecção de LOI — Letter of Intent.

Seções:
  A · Commodity + produtos (multi-select)
  B · Dados do destinatário
  C · Termos comerciais (volume · duração · incoterm)
  D · Destino / porto    (lógica FOB/EXW vs CIF)
  E · Termos financeiros

CSS: injetado num único bloco <style>, sem divs cruzando chamadas separadas.
Tema: dark (consistente com o Control Desk).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.knowledge.loi_dictionary import COMMODITIES, get_commodity


# ─── CSS  (um único bloco, sem misturar com <link>) ──────────────────────────
_FONT  = '<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">'
_STYLE = """<style>
/* ── reset de interface Streamlit para esta view ── */
.stApp                    { background:#F4F5F7 !important; font-family:'Montserrat',sans-serif !important; }
[data-testid="stSidebar"],
[data-testid="collapsedControl"] { display:none !important; }
header, footer            { display:none !important; }
.block-container          { padding:28px 40px 48px !important; max-width:1000px !important; margin:0 auto !important; }
div[data-testid="column"] { padding:0 8px !important; }

/* ── tipografia ── */
label, .stRadio label, .stCheckbox label,
.stSelectbox label, .stMultiSelect label,
.stTextInput label, .stNumberInput label {
    font-family:'Montserrat',sans-serif !important;
    font-size:9px !important; font-weight:700 !important;
    letter-spacing:1.2px !important; color:#7f7f7f !important;
    text-transform:uppercase !important; }

/* ── inputs ── */
.stTextInput input, .stNumberInput input {
    background:#fff !important; color:#262626 !important;
    border:1px solid #E0E0E0 !important;
    border-radius:7px !important;
    font-family:'Montserrat',sans-serif !important;
    font-size:13px !important; }
.stTextInput input:focus, .stNumberInput input:focus {
    border-color:#FA8200 !important;
    box-shadow:0 0 0 2px rgba(250,130,0,.14) !important; }

/* ── selectbox / multiselect ── */
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

/* ── botão primário (Gerar LOI) ── */
div.stButton > button[kind="primaryFormSubmit"],
div.stButton > button[kind="primary"] {
    background:#FA8200 !important; color:#fff !important;
    border:none !important; border-radius:8px !important;
    font-family:'Montserrat',sans-serif !important;
    font-size:12px !important; font-weight:700 !important;
    letter-spacing:1px !important; padding:11px 0 !important; }
div.stButton > button[kind="primary"]:hover { background:#C86600 !important; }

/* ── checkbox ── */
.stCheckbox span { color:#7f7f7f !important;
    font-family:'Montserrat',sans-serif !important; font-size:11px !important; }

/* ── divider ── */
[data-testid="stDivider"] { border-color:#E8E9EC !important; }

/* ── expander ── */
[data-testid="stExpander"] {
    border:1px solid #E8E9EC !important;
    border-radius:8px !important;
    background:#fff !important; }
[data-testid="stExpander"] summary {
    font-family:'Montserrat',sans-serif !important;
    font-size:11px !important; color:#7f7f7f !important; }

/* ── caption / metric ── */
[data-testid="stCaptionContainer"] { color:#7f7f7f !important;
    font-family:'Montserrat',sans-serif !important; font-size:10px !important; }
[data-testid="stMetric"] label     { color:#7f7f7f !important; }
[data-testid="stMetricValue"]      { color:#FA8200 !important;
    font-family:'Montserrat',sans-serif !important; }
</style>"""


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _labels_by_filter(products: list, grade: str | None, ptype: str | None) -> List[str]:
    out = []
    for p in products:
        if grade and p.get("grade") != grade:
            continue
        if ptype and p.get("product_type") != ptype:
            continue
        out.append(p["label"])
    return out


def _prod_by_label(products: list, label: str) -> dict:
    return next((p for p in products if p["label"] == label), {})


def _sec(label: str, title: str) -> None:
    """Cabeçalho de seção — HTML auto-contido, sem divs abertos."""
    st.markdown(
        f'<div style="margin:28px 0 14px">'
        f'<span style="font-size:9px;font-weight:700;letter-spacing:2.5px;'
        f'color:#FA8200;font-family:Montserrat,sans-serif;text-transform:uppercase">'
        f'{label}</span>&nbsp;&nbsp;'
        f'<span style="font-size:15px;font-weight:800;color:#1A1A1A;'
        f'font-family:Montserrat,sans-serif;letter-spacing:-.2px">{title}</span></div>',
        unsafe_allow_html=True,
    )


def _charcount(val: str, limit: int) -> None:
    n   = len(val or "")
    col = "#FA3232" if n > limit else "#404040"
    st.markdown(
        f'<div style="font-size:9px;color:{col};text-align:right;'
        f'margin-top:-6px;margin-bottom:4px;font-family:Montserrat,sans-serif">'
        f'{n}/{limit}</div>',
        unsafe_allow_html=True,
    )


def _helper(text: str) -> None:
    st.markdown(
        f'<div style="font-size:10px;color:#555;font-style:italic;'
        f'margin-top:-8px;margin-bottom:8px;font-family:Montserrat,sans-serif">'
        f'{text}</div>',
        unsafe_allow_html=True,
    )


def _locked_field(label: str, value: str) -> None:
    st.markdown(
        f'<div style="margin-bottom:12px">'
        f'<div style="font-size:9px;font-weight:700;letter-spacing:1.2px;color:#7f7f7f;'
        f'text-transform:uppercase;font-family:Montserrat,sans-serif;margin-bottom:4px">'
        f'{label}</div>'
        f'<div style="background:#F4F5F7;border:1px solid #E0E0E0;'
        f'border-radius:7px;padding:10px 12px;font-size:13px;font-weight:600;'
        f'color:#8F8F8F;font-family:Montserrat,sans-serif">{value}</div></div>',
        unsafe_allow_html=True,
    )


# ─── Render principal ─────────────────────────────────────────────────────────

def render_loi_widget() -> None:
    """Página completa de confecção de LOI (dark theme, Control Desk)."""

    # Garante que dry_run nunca inicia True por herança de sessão anterior
    if "loi_dry_run" not in st.session_state:
        st.session_state["loi_dry_run"] = False

    # ── CSS (duas chamadas separadas — <link> e <style> nunca juntos) ────────
    st.markdown(_FONT,  unsafe_allow_html=True)
    st.markdown(_STYLE, unsafe_allow_html=True)

    logo_path = ROOT / "assets" / "logo.png"

    # ── Header ────────────────────────────────────────────────────────────────
    hc1, hc2, hc3 = st.columns([0.12, 0.68, 0.20])
    with hc1:
        if logo_path.exists():
            st.image(str(logo_path), width=130)
    with hc2:
        st.markdown(
            '<div style="padding:16px 0 8px;font-family:Montserrat,sans-serif">'
            '<div style="font-size:10px;font-weight:700;letter-spacing:3px;color:#FA8200">'
            'GERADOR DE DOCUMENTOS &nbsp;·&nbsp; LOI</div>'
            '<div style="font-size:11px;color:#555;margin-top:6px;font-weight:500">'
            'Letter of Intent &nbsp;·&nbsp; Confecção automatizada &nbsp;·&nbsp; Output PDF</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with hc3:
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        if st.button("← Documentos", key="loi_back"):
            st.session_state.current_view = "documentos"
            st.rerun()

    st.markdown(
        '<div style="height:1px;background:#E8E9EC;margin:0 0 4px"></div>',
        unsafe_allow_html=True,
    )

    # ── Wrapper de conteúdo ───────────────────────────────────────────────────
    # (padding aplicado por CSS no block-container; aqui apenas referência)

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO A · SELEÇÃO DE PRODUTO
    # ══════════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO A", "Seleção de Produto")

    com_codes  = list(COMMODITIES.keys())
    com_labels = [COMMODITIES[c]["label_en"] for c in com_codes]

    com_idx = st.selectbox(
        "Commodity",
        range(len(com_codes)),
        format_func=lambda i: com_labels[i],
        key="loi_com_idx",
    )
    code = com_codes[com_idx]
    com  = get_commodity(code)
    products: list = com["products"]

    # Filtro intermediário VegOil / Cotton
    grade_choice = ptype_choice = None
    if code == "VEGOIL":
        gc1, _ = st.columns([1, 3])
        with gc1:
            grade_choice = st.radio(
                "Grade", ["Crude", "RBD"], horizontal=True, key="loi_grade",
                help="Crude = Refinery Feed · RBD = Fit for Human Consumption",
            )
    elif code == "COTTON":
        gc1, _ = st.columns([1, 3])
        with gc1:
            ptype_choice = st.radio(
                "Tipo", ["Cotton Lint", "Cottonseed Meal"], horizontal=True, key="loi_cotton_type",
            )

    available_labels = _labels_by_filter(products, grade_choice, ptype_choice)
    if not available_labels:
        st.warning("Nenhum produto disponível com esse filtro.")
        return

    # Multi-select — um, vários ou todos
    selected_products: list = st.multiselect(
        "Produtos  (selecione um ou mais)",
        available_labels,
        default=[available_labels[0]],
        key="loi_products",
        help="Múltiplos produtos geram um PDF por produto com os mesmos dados comerciais.",
    )
    if not selected_products:
        st.info("Selecione ao menos um produto para continuar.")
        return

    # Embalagem: apenas quando produto único
    packaging = None
    if len(selected_products) == 1:
        prod0      = _prod_by_label(products, selected_products[0])
        pack_opts  = prod0.get("packaging_options", [])
        if len(pack_opts) == 1:
            packaging = pack_opts[0]
            _locked_field("Embalagem", packaging)
        elif pack_opts:
            packaging = st.selectbox("Embalagem", pack_opts, key="loi_packaging")
    else:
        st.caption(
            f"{len(selected_products)} produtos selecionados — "
            "embalagem padrão de cada produto será utilizada automaticamente."
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO B · DADOS DO DESTINATÁRIO
    # ══════════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO B", "Dados do Destinatário")

    bc1, bc2 = st.columns(2)
    with bc1:
        first_name = st.text_input(
            "First Name  (máx. 15 caracteres)",
            max_chars=15, key="loi_first_name",
            placeholder="Ex.: Ibrahim",
            help="Usado na nomenclatura do arquivo: LOI-SE-{DATA}-{FIRSTNAME}-...",
        )
        _charcount(first_name, 15)
    with bc2:
        full_name = st.text_input(
            "Full Name — DESTINATARY_LOIFULLNAME  (máx. 30)",
            max_chars=30, key="loi_full_name",
            placeholder="Ex.: Ibrahim Trade Co. Ltd.",
        )
        _charcount(full_name, 30)

    bc3, bc4 = st.columns(2)
    with bc3:
        attn = st.text_input(
            "A/C · Attn  (máx. 30 caracteres)",
            max_chars=30, key="loi_attn",
            placeholder="Ex.: Mr. Ahmed Ibrahim",
        )
        _helper("name of the person who will receive the document")
        _charcount(attn, 30)
    with bc4:
        subject = st.text_input(
            "Subject — TEXTO MANUAL A SER INSERIDO  (máx. 40)",
            max_chars=40, key="loi_subject",
            placeholder="Ex.: Frozen Chicken Procurement — Annual Contract",
        )
        _charcount(subject, 40)

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO C · TERMOS COMERCIAIS
    # ══════════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO C", "Termos Comerciais")

    volume_unit = "Containers / FCL" if code in ("CHICKEN", "COTTON") else "MT"
    cc1, cc2, cc3 = st.columns(3)
    with cc1:
        volume = st.number_input(
            f"Volume Mensal  ({volume_unit})",
            min_value=0, value=0, step=1, key="loi_volume",
        )
        if volume > 0:
            st.caption(f"Anual: {volume * 12:,}".replace(",", ".") + f" {volume_unit}")
    with cc2:
        duration = st.radio(
            "Duration",
            ["Spot", "12 Months"],
            horizontal=True,
            key="loi_duration",
        )
    with cc3:
        incoterm = st.radio(
            "Incoterm",
            ["EXW", "FOB", "CIF"],
            horizontal=True,
            key="loi_incoterm",
        )

    price_c1, price_c2 = st.columns([1, 2])
    with price_c1:
        target_price = st.text_input(
            "Target Price  (USD / MT)",
            key="loi_target_price",
            placeholder="Ex.: 320.00",
            help="Preço-alvo por MT em USD. Deixe em branco para omitir do documento.",
        )
        _helper("valor unitário em USD — ex.: 320.00")

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO D · DESTINO E PORTO
    # ══════════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO D", "Destino e Porto de Descarga")

    fob_mode = incoterm in ("FOB", "EXW")

    if fob_mode:
        st.markdown(
            '<div style="font-size:10px;color:#555;font-style:italic;margin-bottom:10px;'
            'font-family:Montserrat,sans-serif">'
            'FOB / EXW — origem Brazil (bloqueado) &nbsp;·&nbsp; '
            'Cidade e Estado obrigatórios</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="font-size:10px;color:#555;font-style:italic;margin-bottom:10px;'
            'font-family:Montserrat,sans-serif">'
            'CIF — País obrigatório &nbsp;·&nbsp; '
            'Cidade, Estado e Porto opcionais</div>',
            unsafe_allow_html=True,
        )

    dc1, dc2, dc3, dc4 = st.columns(4)
    with dc1:
        if fob_mode:
            _locked_field("País *", "Brazil")
            dest_country = "Brazil"
        else:
            dest_country = st.text_input("País *", key="loi_country", placeholder="Ex.: China")
    with dc2:
        req_mark = " *" if fob_mode else ""
        dest_city = st.text_input(f"Cidade{req_mark}", key="loi_city", placeholder="Ex.: Shanghai")
    with dc3:
        dest_state = st.text_input(
            f"Estado / Província{req_mark}", key="loi_state", placeholder="Ex.: Guangdong",
        )
    with dc4:
        dest_port = st.text_input(
            "Porto (opcional)", key="loi_port", placeholder="Ex.: Port of Shanghai",
        )

    # ══════════════════════════════════════════════════════════════════════════
    # SEÇÃO E · TERMOS FINANCEIROS
    # ══════════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO E", "Termos Financeiros")

    ec1, ec2 = st.columns(2)
    with ec1:
        payment_terms = st.radio(
            "Payment Terms",
            ["SBLC (MT760)", "DLC (MT700)"],
            horizontal=True,
            key="loi_payment",
        )
    with ec2:
        perf_bond = st.radio(
            "Performance Bond",
            ["Spot/Trial", "12 months"],
            horizontal=True,
            key="loi_perf_bond",
        )

    # Origem — visível apenas para Sugar e VegOil
    origin_country = None
    extra = com.get("extra_rules") or {}
    if code in ("SUGAR", "VEGOIL") and extra.get("origin_options"):
        oc1, _ = st.columns([1, 2])
        with oc1:
            origin_country = st.selectbox(
                "Origem do Produto",
                extra["origin_options"],
                key="loi_origin",
            )
        if code == "SUGAR" and origin_country and origin_country != "Brazil":
            st.warning(f"Atenção: {extra.get('non_brazil_alert', '')}")

    st.divider()

    # ══════════════════════════════════════════════════════════════════════════
    # AÇÃO FINAL
    # ══════════════════════════════════════════════════════════════════════════

    # dry_run oculto — acessível apenas por devs via expander
    with st.expander("⚙ Opções avançadas", expanded=False):
        dry_run = st.checkbox(
            "Dry-run — validar sem upload (não sobe ao Drive)",
            value=False,
            key="loi_dry_run",
        )
        if dry_run:
            st.warning("Modo dry-run ativo — nenhum arquivo será enviado ao Drive.")

    # Força dry_run=False se o checkbox não foi explicitamente marcado nesta sessão
    dry_run = st.session_state.get("loi_dry_run", False)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    submit = st.button(
        "Gerar LOI →",
        key="loi_submit",
        type="primary",
        use_container_width=True,
    )

    if not submit:
        return

    # ── Validações ────────────────────────────────────────────────────────────
    errors: list[str] = []
    effective_full  = (full_name or first_name or "").strip()
    effective_first = (first_name or (effective_full.split()[0] if effective_full else "")).strip()

    if not effective_full:
        errors.append("Informe First Name ou Full Name do destinatário.")
    if fob_mode:
        if not dest_city:   errors.append("Cidade é obrigatória para FOB/EXW.")
        if not dest_state:  errors.append("Estado/Província é obrigatório para FOB/EXW.")
    else:
        if not dest_country: errors.append("País é obrigatório para CIF.")

    if errors:
        for e in errors:
            st.error(e)
        return

    # ── Monta inputs base ─────────────────────────────────────────────────────
    base_inputs: dict = {
        "DESTINATARY_LOIFULLNAME":      effective_full,
        "DESTINATARY_LOIFIRSTNAME":     effective_first.upper(),
        "ATTN":                         attn or "",
        "TEXTO MANUAL A SER INCERIDO":  subject or "",
        "CITY":                         dest_city or "",
        "STATE":                        dest_state or "",
        "NAME_OF_PORT":                 dest_port or "",
        "COUNTRY":                      dest_country or "",
        "NATIONALITY_OF_DESTINATION":   dest_country or "",
        "VOLUME_MONTHLY":               str(volume) if volume > 0 else "",
        "DURATION":                     duration,
        "INCOTERM":                     incoterm,
        "PAYMENT_TERMS":                payment_terms,
        "PERFORMANCE_BOND":             perf_bond,
        "TARGET_PRICE":                 target_price or "",
    }
    if origin_country:
        base_inputs["ORIGIN_COUNTRY"] = origin_country

    # ── Geração (um PDF por produto selecionado) ──────────────────────────────
    from agents.loi_generator_agent import LOIGeneratorAgent

    n = len(selected_products)
    label_plural = "1 LOI" if n == 1 else f"{n} LOIs"

    results: list[dict] = []
    agent = LOIGeneratorAgent()

    with st.spinner(f"Gerando {label_plural}…"):
        for prod_label in selected_products:
            prod_data  = _prod_by_label(products, prod_label)
            pack_opts  = prod_data.get("packaging_options", [])
            pack_value = (
                packaging
                if (packaging and n == 1)
                else (pack_opts[0] if pack_opts else "")
            )
            call_inputs = {**base_inputs, "PACKAGING": pack_value}

            try:
                res = agent.process({
                    "commodity_code": code,
                    "product_label":  prod_label,
                    "user_inputs":    call_inputs,
                    "dry_run":        dry_run,
                    "output_format":  "pdf",
                })
            except Exception as exc:
                res = {"status": "error", "error": str(exc)}

            res["_product"] = prod_label
            results.append(res)

    # ── Resultados ───────────────────────────────────────────────────────────
    successes = [r for r in results if r.get("status") == "success"]
    errors    = [r for r in results if r.get("status") != "success"]

    if successes and not dry_run:
        count = len(successes)
        label = "1 LOI gerada" if count == 1 else f"{count} LOIs geradas"
        st.markdown(
            f'<div style="background:#f0faf0;border:1px solid #329632;border-radius:10px;'
            f'padding:18px 22px;margin:18px 0 12px;font-family:Montserrat,sans-serif">'
            f'<div style="font-size:13px;font-weight:800;color:#2a7a2a;margin-bottom:4px">'
            f'✓ {label} com sucesso</div>'
            f'<div style="font-size:10px;color:#7f7f7f">Arquivos salvos no Google Drive — '
            f'clique em cada link abaixo para abrir</div></div>',
            unsafe_allow_html=True,
        )

    for res in results:
        prod_label  = res.get("_product", "")
        ok          = res.get("status") == "success"
        border_col  = "#329632" if ok else "#FA3232"
        icon        = "✓" if ok else "✗"
        icon_col    = border_col

        if ok and dry_run:
            body = (
                f"<strong style='color:#1A1A1A'>Dry-run OK</strong> &nbsp;·&nbsp; "
                f"{res.get('filename', '')}<br>"
                f"<span style='font-size:10px;color:#7f7f7f'>"
                f"{res.get('size_bytes', 0):,} bytes &nbsp;·&nbsp; "
                f"{len(res.get('selected_keywords', []))} keywords mantidas</span>"
            )
        elif ok:
            link = res.get("web_link", "")
            body = (
                f"<div style='font-size:13px;font-weight:700;color:#1A1A1A;margin-bottom:6px'>"
                f"{res.get('filename', '')}</div>"
                + (
                    f'<a href="{link}" target="_blank" '
                    f'style="background:#FA8200;color:#fff;font-size:11px;font-weight:700;'
                    f'text-decoration:none;padding:6px 14px;border-radius:5px;display:inline-block">'
                    f'Abrir no Google Drive →</a>'
                    if link else
                    f"<span style='color:#FA3232;font-size:11px'>Link não disponível</span>"
                )
                + f"<div style='font-size:10px;color:#7f7f7f;margin-top:6px'>"
                f"{res.get('size_bytes', 0):,} bytes &nbsp;·&nbsp; formato PDF</div>"
            )
        else:
            err = res.get("error", "erro desconhecido")
            body = (
                f"<div style='font-size:12px;font-weight:700;color:#FA3232;margin-bottom:4px'>"
                f"Falha na geração</div>"
                f"<div style='font-size:11px;color:#262626;word-break:break-all'>{err}</div>"
            )

        st.markdown(
            f'<div style="background:#fff;border-radius:9px;padding:18px 20px;'
            f'border:1px solid #E8E9EC;border-left:4px solid {border_col};'
            f'margin-bottom:6px;font-family:Montserrat,sans-serif">'
            f'<div style="font-size:9px;font-weight:700;letter-spacing:1.5px;'
            f'color:#BFBFBF;margin-bottom:10px;text-transform:uppercase">'
            f'<span style="color:{icon_col}">{icon}</span> &nbsp;'
            f'{code} &nbsp;·&nbsp; {prod_label}</div>'
            f'{body}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Botão de download local do PDF
        if ok and not dry_run and res.get("file_bytes"):
            st.download_button(
                label="⬇  Baixar PDF",
                data=res["file_bytes"],
                file_name=res.get("filename", f"LOI-{code}.pdf"),
                mime="application/pdf",
                key=f"dl_{code}_{prod_label}_{results.index(res)}",
            )

        for alert in res.get("alerts", []):
            st.warning(alert)

        if ok and dry_run:
            with st.expander(f"Detalhes dry-run · {prod_label}"):
                st.markdown("**Keywords mantidas:**")
                st.json(res.get("selected_keywords", []))
                st.markdown("**Keywords descartadas:**")
                st.json(res.get("drop_keywords", []))
                st.markdown("**Chaves simples:**")
                st.json(res.get("simple_keys", []))
