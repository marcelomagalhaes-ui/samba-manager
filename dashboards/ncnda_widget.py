# -*- coding: utf-8 -*-
"""
dashboards/ncnda_widget.py
==========================
Página de confecção de NCNDA — Non-Circumvention, Non-Disclosure &
Confidentiality Agreement.

Seções:
  A · Party I — Samba Export (fixo / bloqueado)
  B · Gestão dinâmica de partes (Party II obrigatória + III e IV opcionais)
  C · Geração e download

CSS/Tema: idêntico ao loi_widget.py (light, #F4F5F7).
"""
from __future__ import annotations

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
.stNumberInput label, .stTextArea label {
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

/* ── botões base ── */
div.stButton > button {
    background:transparent !important; color:#7F7F7F !important;
    border:1px solid #D9D9D9 !important; border-radius:7px !important;
    font-family:'Montserrat',sans-serif !important; font-size:10px !important;
    font-weight:600 !important; letter-spacing:.8px !important; }
div.stButton > button:hover {
    border-color:#FA8200 !important; color:#FA8200 !important; }

/* ── botão primário (Gerar NCNDA) ── */
div.stButton > button[kind="primaryFormSubmit"],
div.stButton > button[kind="primary"] {
    background:#FA8200 !important; color:#fff !important;
    border:none !important; border-radius:8px !important;
    font-family:'Montserrat',sans-serif !important;
    font-size:12px !important; font-weight:700 !important;
    letter-spacing:1px !important; padding:11px 0 !important; }
div.stButton > button[kind="primary"]:hover { background:#C86600 !important; }

/* ── botões de remover parte (vermelho) ── */
.st-key-ncnda_remove_p3 button,
.st-key-ncnda_remove_p4 button {
    background:transparent !important; color:#E03232 !important;
    border:1px solid #FFCCCC !important; border-radius:7px !important;
    font-size:11px !important; font-weight:600 !important; }
.st-key-ncnda_remove_p3 button:hover,
.st-key-ncnda_remove_p4 button:hover {
    background:#FFF0F0 !important; border-color:#E03232 !important; }

/* ── botão adicionar parte (laranja dashed) ── */
.st-key-ncnda_add_party button {
    border:1px dashed #FA8200 !important; color:#FA8200 !important;
    background:#FFFAF5 !important; border-radius:8px !important;
    font-size:11px !important; font-weight:700 !important;
    letter-spacing:.6px !important; width:100% !important; }
.st-key-ncnda_add_party button:hover { background:#FFF3E0 !important; }

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

/* ── language toggle (EN / PTBR) ── */
.st-key-ncnda_language [data-testid="stWidgetLabel"] { display:none !important; }
.st-key-ncnda_language .stRadio > div {
    background:#EBEBED;
    border-radius:10px;
    padding:3px;
    gap:3px !important;
    display:inline-flex !important; }
.st-key-ncnda_language .stRadio > div label {
    background:transparent !important;
    border:none !important;
    border-radius:7px !important;
    padding:7px 24px !important;
    font-size:12px !important;
    font-weight:800 !important;
    color:#7F7F7F !important;
    text-transform:uppercase !important;
    letter-spacing:2px !important;
    cursor:pointer !important;
    transition:all .15s !important;
    margin:0 !important; }
.st-key-ncnda_language .stRadio > div label:has(input:checked) {
    background:#FA8200 !important;
    color:#fff !important;
    box-shadow:0 2px 8px rgba(250,130,0,.28) !important; }
.st-key-ncnda_language .stRadio > div label input { display:none !important; }
</style>"""


# ─── Helpers ─────────────────────────────────────────────────────────────────

_PARTY_ROLES = {
    2: "Intermediary",
    3: "Intermediary",
    4: "Intermediary",
}

_ROMAN = ["", "I", "II", "III", "IV"]


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


def _charcount(val: str, limit: int) -> None:
    n = len(val or "")
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


def _clear_party_keys(n: int) -> None:
    """Remove todas as chaves de sessão para a parte n."""
    for suffix in ("full_name", "short_name", "country", "tax_id",
                   "address", "legal_rep", "passport"):
        key = f"ncnda_p{n}_{suffix}"
        if key in st.session_state:
            del st.session_state[key]


# ─── Bloco dinâmico de partido ────────────────────────────────────────────────

def _render_party_block(n: int, can_remove: bool) -> dict:
    """
    Renderiza o bloco de inputs para a Party n (2, 3 ou 4).
    Retorna dict com os valores dos campos + flag 'removed'.
    """
    role   = _PARTY_ROLES[n]
    roman  = _ROMAN[n]

    # ── Card header (completo, sem divs abertas) ──────────────────────────
    title_col, action_col = st.columns([6, 1])
    with title_col:
        st.markdown(
            f'<div style="margin:28px 0 14px;padding:14px 20px;background:#fff;'
            f'border:1px solid #E0E0E0;border-radius:10px;border-left:4px solid #FA8200;'
            f'font-family:Montserrat,sans-serif">'
            f'<span style="font-size:9px;font-weight:700;letter-spacing:2.5px;color:#FA8200">'
            f'PARTY {roman}</span>'
            f'<span style="font-size:14px;font-weight:800;color:#1A1A1A;margin-left:14px">'
            f'{role}</span></div>',
            unsafe_allow_html=True,
        )
    with action_col:
        removed = False
        if can_remove:
            st.markdown("<div style='height:42px'></div>", unsafe_allow_html=True)
            if st.button(
                "🗑 Remover",
                key=f"ncnda_remove_p{n}",
                help=f"Remover Party {roman}",
            ):
                removed = True

    # ── Row 1: Full Name + Short Name ─────────────────────────────────────
    fa1, fa2 = st.columns(2)
    with fa1:
        full_name = st.text_input(
            "Company Full Name *",
            key=f"ncnda_p{n}_full_name",
            placeholder="Ex.: Jandira Cosméticos S.A.",
        )
    with fa2:
        short_name = st.text_input(
            "Company Short Name (máx. 15 car.) *",
            max_chars=15,
            key=f"ncnda_p{n}_short_name",
            placeholder="Ex.: JANDIRA",
        )
        _charcount(short_name, 15)
        if n == 2:
            _helper("Usado para compor o código de referência do documento")

    # ── Row 2: Country + Tax ID ───────────────────────────────────────────
    fb1, fb2 = st.columns(2)
    with fb1:
        country = st.text_input(
            "Country of Incorporation *",
            key=f"ncnda_p{n}_country",
            placeholder="Ex.: Brazil, UAE, China",
        )
    with fb2:
        tax_id = st.text_input(
            "Company Registration No. (Tax ID) *",
            key=f"ncnda_p{n}_tax_id",
            placeholder="Ex.: 00.000.000/0001-00",
        )

    # ── Row 3: Registered Address (full width) ────────────────────────────
    address = st.text_area(
        "Registered Address *",
        key=f"ncnda_p{n}_address",
        placeholder="Ex.: Av. Paulista, 1000, Suite 201, São Paulo, SP — Brazil",
        height=80,
    )

    # ── Row 4: Legal Rep + Passport ───────────────────────────────────────
    fc1, fc2 = st.columns(2)
    with fc1:
        legal_rep = st.text_input(
            "Legal Representative Name *",
            key=f"ncnda_p{n}_legal_rep",
            placeholder="Ex.: João da Silva",
        )
    with fc2:
        passport = st.text_input(
            "Passport Number *",
            key=f"ncnda_p{n}_passport",
            placeholder="Ex.: AB1234567",
        )

    # ── Separator ─────────────────────────────────────────────────────────
    st.markdown(
        '<div style="height:1px;background:#E8E9EC;margin:20px 0 0"></div>',
        unsafe_allow_html=True,
    )

    return {
        "removed":    removed,
        "full_name":  full_name.strip(),
        "short_name": short_name.strip(),
        "country":    country.strip(),
        "tax_id":     tax_id.strip(),
        "address":    address.strip(),
        "legal_rep":  legal_rep.strip(),
        "passport":   passport.strip(),
    }


# ─── Render principal ─────────────────────────────────────────────────────────

def render_ncnda_widget() -> None:
    """Página completa de confecção de NCNDA (light theme)."""

    # ── Init session state ────────────────────────────────────────────────
    if "ncnda_num_parties" not in st.session_state:
        st.session_state["ncnda_num_parties"] = 1  # 1 = somente Party II
    if "ncnda_dry_run" not in st.session_state:
        st.session_state["ncnda_dry_run"] = False
    if "ncnda_language" not in st.session_state:
        st.session_state["ncnda_language"] = "EN"

    # ── CSS ───────────────────────────────────────────────────────────────
    st.markdown(_FONT,  unsafe_allow_html=True)
    st.markdown(_STYLE, unsafe_allow_html=True)

    logo_path = ROOT / "assets" / "logo.png"

    # ═══════════════════════════════════════════════════════════════════════
    # HEADER
    # ═══════════════════════════════════════════════════════════════════════
    hc1, hc2, hc3 = st.columns([0.18, 0.62, 0.20])
    with hc1:
        if logo_path.exists():
            st.markdown("<div style='padding:10px 0 0'></div>", unsafe_allow_html=True)
            st.image(str(logo_path), width=180)
    with hc2:
        st.markdown(
            '<div style="padding:14px 0 8px;font-family:Montserrat,sans-serif">'
            '<div style="font-size:10px;font-weight:700;letter-spacing:3px;color:#FA8200">'
            'GERADOR DE DOCUMENTOS &nbsp;·&nbsp; NCNDA</div>'
            '<div style="font-size:11px;color:#555;margin-top:6px;font-weight:500">'
            'Non-Circumvention · Non-Disclosure &amp; Confidentiality Agreement'
            ' &nbsp;·&nbsp; Output PDF</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with hc3:
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        if st.button("← Documentos", key="ncnda_back"):
            st.session_state.current_view = "documentos"
            st.rerun()

    st.markdown(
        '<div style="height:1px;background:#E8E9EC;margin:0 0 4px"></div>',
        unsafe_allow_html=True,
    )

    # ═══════════════════════════════════════════════════════════════════════
    # SELETOR DE IDIOMA — topo do conteúdo, antes de qualquer campo
    # ═══════════════════════════════════════════════════════════════════════
    lang_label_col, lang_toggle_col, lang_info_col = st.columns([0.18, 0.28, 0.54])
    with lang_label_col:
        st.markdown(
            '<div style="padding:18px 0 0;font-size:9px;font-weight:700;letter-spacing:2px;'
            'color:#7f7f7f;font-family:Montserrat,sans-serif;text-transform:uppercase">'
            'Idioma do documento</div>',
            unsafe_allow_html=True,
        )
    with lang_toggle_col:
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
        language = st.radio(
            "Idioma",
            ["EN", "PTBR"],
            index=0 if st.session_state["ncnda_language"] == "EN" else 1,
            horizontal=True,
            key="ncnda_language",
            label_visibility="hidden",
        )
    with lang_info_col:
        lang_desc = (
            "English · International template"
            if language == "EN"
            else "Português BR · Template nacional"
        )
        st.markdown(
            f'<div style="padding:18px 0 0;font-size:10px;color:#ACACAC;'
            f'font-family:Montserrat,sans-serif;font-style:italic">'
            f'{lang_desc}</div>',
            unsafe_allow_html=True,
        )

    st.markdown(
        '<div style="height:1px;background:#E8E9EC;margin:12px 0 0"></div>',
        unsafe_allow_html=True,
    )

    # ═══════════════════════════════════════════════════════════════════════
    # SEÇÃO A · PARTY I — ORIGINADOR (FIXO)
    # ═══════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO A", "Party I — Originator")

    st.markdown(
        '<div style="background:#fff;border:1px solid #E0E0E0;border-radius:10px;'
        'border-left:4px solid #FA8200;padding:18px 22px;margin-bottom:8px;'
        'font-family:Montserrat,sans-serif">'
        '<div style="display:flex;align-items:center;gap:6px;margin-bottom:12px">'
        '<span style="font-size:9px;font-weight:700;letter-spacing:2.5px;color:#FA8200">'
        'PARTY I · ORIGINATOR</span>'
        '<span style="font-size:9px;background:#F4F5F7;color:#8F8F8F;border:1px solid #E0E0E0;'
        'border-radius:4px;padding:1px 7px;letter-spacing:.5px;font-weight:600">BLOQUEADO</span>'
        '</div>'
        '<div style="display:flex;gap:48px;flex-wrap:wrap">'
        '<div>'
        '<div style="font-size:9px;color:#7f7f7f;font-weight:700;letter-spacing:1px;'
        'margin-bottom:3px;text-transform:uppercase">Empresa</div>'
        '<div style="font-size:15px;font-weight:800;color:#1A1A1A">SAMBA EXPORT LTDA</div>'
        '</div>'
        '<div>'
        '<div style="font-size:9px;color:#7f7f7f;font-weight:700;letter-spacing:1px;'
        'margin-bottom:3px;text-transform:uppercase">CNPJ</div>'
        '<div style="font-size:13px;font-weight:600;color:#555">60.280.015/0001-82</div>'
        '</div>'
        '<div>'
        '<div style="font-size:9px;color:#7f7f7f;font-weight:700;letter-spacing:1px;'
        'margin-bottom:3px;text-transform:uppercase">Role</div>'
        '<div style="font-size:13px;font-weight:600;color:#555">'
        'Brazilian Commodity Originator &amp; Exporter</div>'
        '</div>'
        '</div>'
        '<div style="margin-top:10px;font-size:10px;color:#ACACAC;border-top:1px solid #F0F0F0;'
        'padding-top:10px">'
        'Av. Brigadeiro Faria Lima, 1811, Suite 115 · São Paulo, SP — Brazil'
        ' &nbsp;·&nbsp; Rep.: Marcelo Soares Magalhães Nogueira'
        '</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # ═══════════════════════════════════════════════════════════════════════
    # SEÇÃO B · PARTES ADICIONAIS (dinâmico)
    # ═══════════════════════════════════════════════════════════════════════
    _sec("SEÇÃO B", "Partes Adicionais")

    st.markdown(
        '<div style="font-size:10px;color:#7f7f7f;font-style:italic;'
        'margin-bottom:8px;font-family:Montserrat,sans-serif">'
        'Party II é obrigatória. Adicione até mais 2 partes (Party III e IV) conforme necessário.'
        '</div>',
        unsafe_allow_html=True,
    )

    num = st.session_state["ncnda_num_parties"]  # 1, 2 ou 3
    parties_data: list[dict] = []

    # ── Party II (sempre) ─────────────────────────────────────────────────
    p2 = _render_party_block(2, can_remove=False)
    parties_data.append(p2)

    # ── Party III (opcional) ──────────────────────────────────────────────
    if num >= 2:
        p3 = _render_party_block(3, can_remove=(num == 2))
        if p3["removed"]:
            st.session_state["ncnda_num_parties"] = 1
            _clear_party_keys(3)
            st.rerun()
        parties_data.append(p3)

    # ── Party IV (opcional) ───────────────────────────────────────────────
    if num >= 3:
        p4 = _render_party_block(4, can_remove=True)
        if p4["removed"]:
            st.session_state["ncnda_num_parties"] = 2
            _clear_party_keys(4)
            st.rerun()
        parties_data.append(p4)

    # ── Botão "+ Adicionar Parte" ─────────────────────────────────────────
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    btn_col, _ = st.columns([1, 1])
    with btn_col:
        if num >= 3:
            st.markdown(
                '<div style="background:#F4F5F7;border:1px dashed #D9D9D9;'
                'border-radius:8px;padding:11px 18px;text-align:center;'
                'font-size:10px;font-weight:700;color:#BFBFBF;'
                'font-family:Montserrat,sans-serif;letter-spacing:.8px">'
                '+ ADICIONAR PARTE &nbsp; (máximo 3 atingido)'
                '</div>',
                unsafe_allow_html=True,
            )
        else:
            next_idx  = num + 2   # num=1→III(3), num=2→IV(4)
            next_role = _PARTY_ROLES[next_idx]
            next_rom  = _ROMAN[next_idx]
            if st.button(
                f"+ Adicionar Parte  (Party {next_rom} · {next_role})",
                key="ncnda_add_party",
            ):
                st.session_state["ncnda_num_parties"] = num + 1
                st.rerun()

    st.divider()

    # ═══════════════════════════════════════════════════════════════════════
    # SEÇÃO C · GERAÇÃO
    # ═══════════════════════════════════════════════════════════════════════

    with st.expander("⚙ Opções avançadas", expanded=False):
        dry_run = st.checkbox(
            "Dry-run — validar sem upload (não sobe ao Drive)",
            value=False,
            key="ncnda_dry_run",
        )
        if dry_run:
            st.warning("Modo dry-run ativo — nenhum arquivo será enviado ao Drive.")

    dry_run = st.session_state.get("ncnda_dry_run", False)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    submit = st.button(
        "Gerar NCNDA →",
        key="ncnda_submit",
        type="primary",
        use_container_width=True,
    )

    if not submit:
        return

    # ── Validações ────────────────────────────────────────────────────────
    errors: list[str] = []
    for i, p in enumerate(parties_data, start=2):
        roman = _ROMAN[i]
        required = {
            "Company Full Name":     p["full_name"],
            "Company Short Name":    p["short_name"],
            "Country of Incorporation": p["country"],
            "Tax ID":                p["tax_id"],
            "Registered Address":    p["address"],
            "Legal Representative":  p["legal_rep"],
            "Passport Number":       p["passport"],
        }
        for fname, fval in required.items():
            if not fval:
                errors.append(f"Party {roman} · {fname} é obrigatório.")

    if errors:
        for e in errors:
            st.error(e)
        return

    # ── Monta payload ─────────────────────────────────────────────────────
    language = st.session_state.get("ncnda_language", "EN")
    n_additional = len(parties_data)  # 1, 2 ou 3
    # template_prefix: EN → "1"/"2"/"3" | PTBR → "1PT"/"2PT"/"3PT"
    template_prefix = (
        f"{n_additional}PT" if language == "PTBR" else str(n_additional)
    )

    payload_parties = []
    for i, p in enumerate(parties_data, start=2):
        payload_parties.append({
            "party_num":   i,
            "party_roman": _ROMAN[i],
            "role":        _PARTY_ROLES[i],
            "full_name":   p["full_name"],
            "short_name":  p["short_name"],
            "country":     p["country"],
            "tax_id":      p["tax_id"],
            "address":     p["address"],
            "legal_rep":   p["legal_rep"],
            "passport":    p["passport"],
        })

    # ── Chama o agente ────────────────────────────────────────────────────
    from agents.ncnda_generator_agent import NCNDAGeneratorAgent

    agent = NCNDAGeneratorAgent()
    with st.spinner(f"Gerando NCNDA ({language})…"):
        try:
            res = agent.process({
                "document_type":   "NCNDA",
                "language":        language,
                "template_prefix": template_prefix,
                "parties":         payload_parties,
                "dry_run":         dry_run,
            })
        except Exception as exc:
            res = {"status": "error", "error": str(exc)}

    # ── Resultado ─────────────────────────────────────────────────────────
    ok         = res.get("status") == "success"
    border_col = "#329632" if ok else "#FA3232"
    icon       = "✓" if ok else "✗"
    n_total    = len(payload_parties) + 1   # +1 = Samba (Party I)

    if ok and not dry_run:
        st.markdown(
            '<div style="background:#f0faf0;border:1px solid #329632;border-radius:10px;'
            'padding:18px 22px;margin:18px 0 12px;font-family:Montserrat,sans-serif">'
            '<div style="font-size:13px;font-weight:800;color:#2a7a2a;margin-bottom:4px">'
            '✓ NCNDA gerada com sucesso</div>'
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

    lang_badge = language if ok else st.session_state.get("ncnda_language", "EN")
    tpl_used   = res.get("template_used", "")
    tpl_badge  = f" &nbsp;·&nbsp; {tpl_used}" if tpl_used and tpl_used != "programmatic" else ""
    st.markdown(
        f'<div style="background:#fff;border-radius:9px;padding:18px 20px;'
        f'border:1px solid #E8E9EC;border-left:4px solid {border_col};'
        f'margin-bottom:6px;font-family:Montserrat,sans-serif">'
        f'<div style="font-size:9px;font-weight:700;letter-spacing:1.5px;'
        f'color:#BFBFBF;margin-bottom:10px;text-transform:uppercase">'
        f'<span style="color:{border_col}">{icon}</span> &nbsp;'
        f'NCNDA &nbsp;·&nbsp; {lang_badge} &nbsp;·&nbsp; {n_total} partes{tpl_badge}</div>'
        f'{body}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Botão de download local
    if ok and not dry_run and res.get("file_bytes"):
        st.download_button(
            label="⬇  Baixar PDF",
            data=res["file_bytes"],
            file_name=res.get("filename", "NCNDA-SAMBA.pdf"),
            mime="application/pdf",
            key="ncnda_download",
        )

    for alert in res.get("alerts", []):
        st.warning(alert)
