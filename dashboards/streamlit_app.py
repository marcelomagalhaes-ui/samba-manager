"""
dashboards/streamlit_app.py
===========================
Painel Gerencial da Samba Export - V3 INSTITUCIONAL
"""
import os
import sys
import json
import datetime
from pathlib import Path
import streamlit as st
import pandas as pd
import sqlalchemy

# ── Patch preemptivo: converte tokenize.TokenError → OSError ──────────────────
# O Streamlit 1.x captura (OSError, TypeError) em _make_function_key mas NÃO
# captura tokenize.TokenError.  Em alguns ambientes Cloud, inspect.getsource()
# levanta TokenError ("EOF in multi-line statement") e o app crasha.
# Este patch faz com que qualquer TokenError seja re-levantado como OSError,
# ativando o fallback interno do Streamlit (hash por bytecode em vez de source).
import inspect as _inspect_mod
import tokenize as _tokenize_mod

_orig_getsource = _inspect_mod.getsource

def _safe_getsource(obj):
    try:
        return _orig_getsource(obj)
    except _tokenize_mod.TokenError as _te:
        raise OSError(f"tokenize.TokenError wrapped for st.cache_data: {_te}") from _te

_inspect_mod.getsource = _safe_getsource
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Load .env so DASH_PASSWORD and other vars are available
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(ROOT / ".env", override=True)
except Exception:
    pass

# Injeta secrets do Streamlit Cloud no os.environ
try:
    _secrets_keys = ["DATABASE_URL", "GEMINI_API_KEY", "DASH_PASSWORD",
                     "GOOGLE_CREDENTIALS_JSON", "GOOGLE_TOKEN_JSON"]
    for _sk in _secrets_keys:
        try:
            _sv = st.secrets[_sk]
            if isinstance(_sv, str):
                os.environ[_sk] = _sv
        except Exception:
            pass
except Exception:
    pass

from models.database import get_engine, get_session, create_tables
try:
    from services.market_data import market_data, PhysicalMarketScraper
except Exception:
    market_data = None
    PhysicalMarketScraper = None

# Garante que o banco existe (necessário no Streamlit Cloud onde o SQLite começa vazio)
try:
    _db_url = os.getenv("DATABASE_URL", "sqlite:///data/samba_control.db")
    Path("data").mkdir(exist_ok=True)
    create_tables(_db_url)
except Exception:
    pass

# CONFIG
st.set_page_config(
    page_title="Samba Export | Plataforma Corporativa",
    page_icon="🟠",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ═══════════════════════════════════════════════════════════════════
# AUTH — Login gate, portal router
# ═══════════════════════════════════════════════════════════════════
import hashlib as _hashlib
import os as _os

_ALLOWED_DOMAINS = {"sambaexport.com.br"}
_ALLOWED_EMAILS  = {
    "msmaganog@gmail.com",
    "lbd@sambaexport.com.br",
    "nmd@sambaexport.com.br",
    "marcelo.magalhaes@sambaexport.com.br",
}
_DASH_PWD_HASH   = _hashlib.sha256(
    _os.getenv("DASH_PASSWORD", "samba@2026").encode()
).hexdigest()
_AUTH_LOGO = ROOT / "assets" / "logo.png"

def _email_ok(email: str) -> bool:
    e = email.strip().lower()
    domain = e.split("@")[-1] if "@" in e else ""
    return domain in _ALLOWED_DOMAINS or e in _ALLOWED_EMAILS

def _pwd_ok(pwd: str) -> bool:
    return _hashlib.sha256(pwd.encode()).hexdigest() == _DASH_PWD_HASH

def _init_session():
    for k, v in {
        "authenticated": False,
        "user_email":    "",
        "user_name":     "",
        "current_view":  "portal",
        "prev_view":     "portal",
        "auth_error":    "",
    }.items():
        if k not in st.session_state:
            st.session_state[k] = v

# ─── LOGIN PAGE ──────────────────────────────────────────────────
def _show_login():
    import base64 as _b64
    _logo_uri = ""
    if _AUTH_LOGO.exists():
        _logo_uri = "data:image/png;base64," + _b64.b64encode(_AUTH_LOGO.read_bytes()).decode()

    st.markdown(f"""
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
.stApp {{ background: #F0F1F5 !important; font-family: 'Montserrat', sans-serif !important; }}
[data-testid="stSidebar"], [data-testid="collapsedControl"], [data-testid="stToolbar"], [data-testid="stHeader"], [data-testid="stDecoration"] {{ display: none !important; }}
header, footer {{ display: none !important; }}
.block-container {{ padding-top: 0 !important; padding-bottom: 0 !important; max-width: 480px !important; }}
.stTextInput input {{
    background: #fff !important; color: #262626 !important;
    border: 1px solid #E0E0E0 !important; border-radius: 8px !important;
    font-family: 'Montserrat', sans-serif !important; font-size: 14px !important; padding: 12px !important;
}}
.stTextInput input:focus {{ border-color: #FA8200 !important; box-shadow: 0 0 0 3px rgba(250,130,0,.12) !important; }}
.stTextInput label {{ color: #7F7F7F !important; font-size: 10px !important; font-weight: 700 !important;
    letter-spacing: 1.2px !important; font-family: 'Montserrat', sans-serif !important; }}
[data-testid="InputInstructions"] {{ display: none !important; }}
div.stButton > button {{
    width: 100% !important; background: #FA8200 !important; color: #fff !important;
    border: none !important; border-radius: 8px !important; padding: 13px !important;
    font-family: 'Montserrat', sans-serif !important; font-size: 13px !important;
    font-weight: 700 !important; letter-spacing: 1.2px !important; margin-top: 8px !important;
}}
div.stButton > button:hover {{ background: #C86600 !important; }}
.login-hero {{
    text-align:center; padding:48px 0 28px; font-family:Montserrat,sans-serif;
}}
.login-hero img {{ width:220px; height:auto; display:block; margin:0 auto 18px; }}
.login-hero .sub {{ font-size:11px; font-weight:700; letter-spacing:3px; color:#FA8200; margin-bottom:8px; }}
.login-hero .desc {{ font-size:12px; color:#9aa0a6; font-weight:500; }}
.login-foot {{ text-align:center; margin-top:28px; font-family:Montserrat,sans-serif;
    font-size:10px; color:#BFBFBF; letter-spacing:.8px; }}
</style>
<div class="login-hero">
  {f'<img src="{_logo_uri}" alt="Samba Export"/>' if _logo_uri else ''}
  <div class="sub">PLATAFORMA CORPORATIVA</div>
  <div class="desc">Acesso restrito · Usuários autorizados</div>
</div>
""", unsafe_allow_html=True)

    email_v = st.text_input("E-MAIL", placeholder="seu@sambaexport.com.br", key="login_email")
    pwd_v   = st.text_input("SENHA", type="password", placeholder="••••••••", key="login_pwd")

    if st.button("ENTRAR →", key="login_submit"):
        st.session_state.auth_error = ""
        if not email_v:
            st.session_state.auth_error = "Informe o e-mail."
        elif not _email_ok(email_v):
            st.session_state.auth_error = "E-mail não autorizado nesta plataforma."
        elif not _pwd_ok(pwd_v):
            st.session_state.auth_error = "Senha incorreta."
        else:
            st.session_state.authenticated = True
            st.session_state.user_email    = email_v.strip().lower()
            name = email_v.split("@")[0].replace(".", " ").title()
            st.session_state.user_name     = name
            st.session_state.current_view  = "portal"
            st.rerun()

    if st.session_state.get("auth_error"):
        st.error(st.session_state.auth_error)

    st.markdown("""<div class="login-foot">SAMBA EXPORT © 2026 · Uso interno restrito</div>""",
                unsafe_allow_html=True)


# ─── PORTAL PAGE — Enterprise design, Brand Manual v3 ────────────

# SVG icons — clean geometric, no emoji
_P_SVG = {
    "ADM": '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="7" width="9" height="14"/><rect x="13" y="11" width="9" height="10"/><polyline points="2 7 12 2 22 7"/><line x1="2" y1="21" x2="22" y2="21"/></svg>',
    "TI":  '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="2" width="8" height="8" rx="1.5"/><rect x="14" y="2" width="8" height="8" rx="1.5"/><rect x="2" y="14" width="8" height="8" rx="1.5"/><rect x="14" y="14" width="8" height="8" rx="1.5"/></svg>',
    "COM": '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>',
    "OPS": '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
    "HUB": '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/><line x1="9" y1="7" x2="15" y2="7"/><line x1="9" y1="11" x2="15" y2="11"/><line x1="9" y1="15" x2="12" y2="15"/></svg>',
}

def _env_card(code, title, desc, modules, color, sub_status=None):
    """Build enterprise environment card — no emoji, clean SVG, brand palette."""
    svg = _P_SVG.get(code, "")
    if sub_status:
        def _mod_row(m, a, col):
            bg   = col if a else "#D9D9D9"
            fw   = "600" if a else "400"
            fc   = "#262626" if a else "#BFBFBF"
            badge = f'<span style="margin-left:auto;font-size:9px;font-weight:700;letter-spacing:.5px;color:{col}">ATIVO</span>' if a else ""
            return (f'<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid #F4F4F4">'
                    f'<span style="width:7px;height:7px;border-radius:50%;flex-shrink:0;background:{bg}"></span>'
                    f'<span style="font-size:11px;font-weight:{fw};color:{fc};font-family:Montserrat,sans-serif">{m}</span>'
                    f'{badge}</div>')
        rows = "".join(_mod_row(m, a, color) for m, a in sub_status)
    else:
        rows = "".join(
            f'<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid #F4F4F4">'
            f'<span style="width:7px;height:7px;border-radius:50%;background:#D9D9D9;flex-shrink:0"></span>'
            f'<span style="font-size:11px;font-weight:400;color:#BFBFBF;font-family:Montserrat,sans-serif">{m}</span>'
            f"</div>"
            for m in modules
        )
    has_active = sub_status and any(a for _, a in sub_status)
    status = (
        f'<span style="font-size:9px;font-weight:700;letter-spacing:.3px;color:#329632">&#9679; Disponível</span>'
        if has_active else
        f'<span style="font-size:9px;font-weight:500;color:#BFBFBF">Em desenvolvimento</span>'
    )
    return f"""
<div style="background:#fff;border-radius:10px;padding:22px 22px 16px;
  border:1px solid #E8E9EC;border-top:3px solid {color};
  box-shadow:0 1px 6px rgba(0,0,0,0.05);height:100%;font-family:Montserrat,sans-serif">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:18px">
    <div style="display:flex;align-items:center;gap:12px">
      <div style="width:40px;height:40px;border-radius:8px;background:{color}16;
        display:flex;align-items:center;justify-content:center;color:{color};flex-shrink:0">
        {svg}
      </div>
      <div>
        <div style="font-size:9px;font-weight:700;letter-spacing:2.2px;color:{color};margin-bottom:3px">{code}</div>
        <div style="font-size:14px;font-weight:800;color:#1A1A1A;line-height:1.2">{title}</div>
      </div>
    </div>
    <div style="margin-top:2px">{status}</div>
  </div>
  <div style="font-size:11px;color:#7F7F7F;margin-bottom:16px;line-height:1.55">{desc}</div>
  <div style="border-top:1px solid #F0F0F0;padding-top:12px">{rows}</div>
</div>"""

def _show_portal():
    st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
.stApp { background: #F4F5F7 !important; font-family: 'Montserrat', sans-serif !important; }
[data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none !important; }
header { display: none !important; }
.block-container { padding: 0 !important; max-width: 1200px !important; margin: 0 auto !important; }

/* Portal-specific buttons */
.portal-btn > div > button {
    background: #FA8200 !important; color: #fff !important; border: none !important;
    border-radius: 6px !important; font-family: Montserrat, sans-serif !important;
    font-size: 10px !important; font-weight: 700 !important; letter-spacing: 1px !important;
    padding: 8px 20px !important;
}
.portal-btn > div > button:hover { background: #C86600 !important; }
div.stButton > button {
    background: transparent !important; color: #7F7F7F !important;
    border: 1px solid #D9D9D9 !important; border-radius: 6px !important;
    font-family: Montserrat, sans-serif !important; font-size: 10px !important;
    font-weight: 600 !important; letter-spacing: .8px !important; padding: 7px 16px !important;
}
div.stButton > button:hover { border-color: #FA8200 !important; color: #FA8200 !important; }
div.stButton > button:disabled { color: #BFBFBF !important; border-color: #EBEBEB !important; cursor: default !important; }
div[data-testid="column"] { padding: 0 8px !important; }
</style>
""", unsafe_allow_html=True)

    # ── Portal wrapper ────────────────────────────────────────────
    st.markdown("""
<div style="background:#fff;border-bottom:1px solid #E8E9EC;padding:0 40px">
""", unsafe_allow_html=True)

    # Header — logo + label "PLATAFORMA CORPORATIVA" (sem texto sambaEXPORT redundante)
    hc1, hc2, hc3, hc4 = st.columns([0.18, 0.44, 0.24, 0.14])
    with hc1:
        if _AUTH_LOGO.exists():
            st.image(str(_AUTH_LOGO), width=160)
    with hc2:
        st.markdown(f"""
<div style="font-family:Montserrat,sans-serif;padding:18px 0 10px">
  <div style="font-size:11px;font-weight:700;letter-spacing:3px;color:#FA8200">PLATAFORMA CORPORATIVA</div>
  <div style="font-size:10px;color:#9aa0a6;margin-top:8px;font-weight:500">
    {st.session_state.user_name}
    <span style="margin:0 8px;color:#E8E9EC">|</span>
    {st.session_state.user_email}
    <span style="margin:0 8px;color:#E8E9EC">|</span>
    {datetime.datetime.now().strftime('%d/%m/%Y  %H:%M')}
  </div>
</div>""", unsafe_allow_html=True)
    with hc3:
        # ── quick-action icons: docs (Streamlit btn) + 3 HTML icons ──────
        st.markdown("""
<style>
/* Docs icon — Streamlit button disfarçado de ícone */
.st-key-portal_docs_nav{margin:0 !important;padding-top:4px !important}
.st-key-portal_docs_nav>div{margin:0 !important}
.st-key-portal_docs_nav button{
  width:36px !important;height:36px !important;min-height:0 !important;
  padding:0 !important;font-size:0 !important;color:transparent !important;
  background:#F4F5F7 url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' stroke='%237F7F7F' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round' viewBox='0 0 24 24'%3E%3Cpath d='M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z'/%3E%3Cpolyline points='14 2 14 8 20 8'/%3E%3Cline x1='16' y1='13' x2='8' y2='13'/%3E%3Cline x1='16' y1='17' x2='8' y2='17'/%3E%3C/svg%3E") no-repeat center/16px !important;
  border:1px solid #E0E0E0 !important;border-radius:8px !important}
.st-key-portal_docs_nav button:hover{
  background-color:#FFF8F0 !important;border-color:#FA8200 !important}
/* HTML icons (pasta, engrenagem, sino) */
.qact-row{display:flex;gap:8px;align-items:center;padding-top:4px}
.qact-btn{width:36px;height:36px;border-radius:8px;background:#F4F5F7;
  border:1px solid #E0E0E0;display:inline-flex;align-items:center;
  justify-content:center;cursor:pointer;text-decoration:none;color:#7F7F7F;
  transition:all .15s;flex-shrink:0}
.qact-btn:hover{border-color:#FA8200;color:#FA8200;background:#FFF8F0}
.qact-btn svg{width:16px;height:16px}
</style>""", unsafe_allow_html=True)
        c_doc, c_icons = st.columns([1, 3])
        with c_doc:
            if st.button("docs", key="portal_docs_nav", help="Gerador de Documentos"):
                st.session_state.prev_view = "portal"
                st.session_state.current_view = "documentos"
                st.rerun()
        with c_icons:
            st.markdown("""<div class="qact-row">
  <a href="https://drive.google.com/drive/folders/0AOllQoxhuNj4Uk9PVA" target="_blank"
    title="Google Drive Corporativo" class="qact-btn">
    <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"
      stroke-linejoin="round" viewBox="0 0 24 24">
      <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
    </svg>
  </a>
  <span class="qact-btn" title="Em breve" style="opacity:.4;cursor:default">
    <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"
      stroke-linejoin="round" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="3"/>
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06
        a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09
        A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83
        l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09
        A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83
        l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09
        a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83
        l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09
        a1.65 1.65 0 0 0-1.51 1z"/>
    </svg>
  </span>
  <span class="qact-btn" title="Em breve" style="opacity:.4;cursor:default">
    <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"
      stroke-linejoin="round" viewBox="0 0 24 24">
      <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
      <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
    </svg>
  </span>
</div>""", unsafe_allow_html=True)
    with hc4:
        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        if st.button("Sair", key="portal_logout"):
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Content area ─────────────────────────────────────────────
    st.markdown("""<div style="padding:28px 40px 48px">""", unsafe_allow_html=True)

    # Section label — clean, no banner
    st.markdown(f"""
<div style="margin-bottom:24px">
  <div style="font-size:9px;font-weight:700;letter-spacing:2.5px;color:#BFBFBF;
    font-family:Montserrat,sans-serif;margin-bottom:6px">AMBIENTES DA PLATAFORMA</div>
  <div style="font-size:20px;font-weight:800;color:#1A1A1A;font-family:Montserrat,sans-serif;
    letter-spacing:-.3px">Selecione um ambiente</div>
  <div style="font-size:11px;color:#7F7F7F;font-family:Montserrat,sans-serif;margin-top:4px">
    Gestão corporativa integrada · Operações de commodities · Base Google Drive
  </div>
</div>
""", unsafe_allow_html=True)

    # ── Row 1 ────────────────────────────────────────────────────
    c1, c2 = st.columns(2, gap="medium")
    with c1:
        st.markdown(_env_card(
            "ADM", "Administração",
            "Gestão corporativa interna, financeira e de pessoas",
            ["Marketing", "Recursos Humanos", "Financeiro", "Contabilidade", "Contas a Pagar"],
            "#FA8200"
        ), unsafe_allow_html=True)
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.button("Em desenvolvimento", key="p_adm", disabled=True)

    with c2:
        st.markdown(_env_card(
            "TI", "Tecnologia",
            "Infraestrutura, sistemas corporativos e inteligência de dados",
            ["Sistemas", "Infraestrutura", "Dados & BI", "Agentes IA"],
            "#326496"
        ), unsafe_allow_html=True)
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.button("Em desenvolvimento", key="p_ti", disabled=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── Row 2 ────────────────────────────────────────────────────
    c3, c4 = st.columns(2, gap="medium")
    with c3:
        st.markdown(_env_card(
            "COM", "Comercial / Prospecção",
            "CRM, gestão de deals e inteligência comercial",
            [],
            "#329632",
            sub_status=[
                ("CRM & Deals",         True),
                ("WhatsApp Agents",     False),
                ("Prospecção",          False),
                ("NCNDA / Documentos",  False),
            ]
        ), unsafe_allow_html=True)
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if st.button("Acessar Comercial", key="p_com"):
            st.session_state.current_view = "comercial"
            st.rerun()

    with c4:
        st.markdown(_env_card(
            "OPS", "Operações",
            "Trading, controle operacional e formação de pricing",
            [],
            "#FA8200",
            sub_status=[
                ("Control Desk — Global Commodities", True),
                ("Trading Desk & Formação de Pricing",  True),
                ("Logística & Supply Chain",            False),
                ("Câmbio & Instrumentos Financeiros",  False),
            ]
        ), unsafe_allow_html=True)
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        if st.button("Acessar Control Desk", key="p_ops"):
            st.session_state.current_view = "operacoes"
            st.rerun()

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── Row 3: Hub (full width) ───────────────────────────────────
    svg_hub = _P_SVG["HUB"]
    st.markdown(f"""
<div style="background:#fff;border-radius:10px;padding:22px 24px;
  border:1px solid #E8E9EC;border-top:3px solid #64C8FA;
  box-shadow:0 1px 6px rgba(0,0,0,0.05);font-family:Montserrat,sans-serif;
  display:flex;align-items:flex-start;gap:18px">
  <div style="width:40px;height:40px;border-radius:8px;background:#64C8FA18;
    display:flex;align-items:center;justify-content:center;color:#326496;flex-shrink:0">
    {svg_hub}
  </div>
  <div style="flex:1">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px">
      <span style="font-size:9px;font-weight:700;letter-spacing:2.2px;color:#64C8FA">HUB</span>
      <span style="font-size:14px;font-weight:800;color:#1A1A1A">Ambiente Interativo</span>
      <span style="margin-left:auto;font-size:9px;color:#2e7d32;font-weight:700;background:#E8F5E9;padding:2px 8px;border-radius:10px;">ATIVO</span>
    </div>
    <div style="font-size:11px;color:#7F7F7F;margin-bottom:14px;line-height:1.55">
      Hub da equipe — scripts LinkedIn, mapa de leads, filtro comprador, FAQ vendas, FAQ commodities, glossário e regras CRM
    </div>
    <div style="display:flex;gap:24px;flex-wrap:wrap">
      <span style="font-size:10px;color:#FA8200;font-weight:600">Scripts</span>
      <span style="color:#E8E9EC">·</span>
      <span style="font-size:10px;color:#FA8200;font-weight:600">Mapa de Leads</span>
      <span style="color:#E8E9EC">·</span>
      <span style="font-size:10px;color:#FA8200;font-weight:600">FAQ Vendas</span>
      <span style="color:#E8E9EC">·</span>
      <span style="font-size:10px;color:#FA8200;font-weight:600">Glossário</span>
      <span style="color:#E8E9EC">·</span>
      <span style="font-size:10px;color:#FA8200;font-weight:600">Regras CRM</span>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    if st.button("▶  Acessar Hub da Equipe", key="p_hub", use_container_width=False):
        st.session_state.current_view = "hub"
        st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)  # /content

    # Footer
    st.markdown(f"""
<div style="text-align:center;font-family:Montserrat,sans-serif;font-size:9px;
  color:#D9D9D9;letter-spacing:1px;padding:24px 0 16px">
  SAMBA EXPORT &nbsp;·&nbsp; {datetime.datetime.now().year} &nbsp;·&nbsp; USO INTERNO RESTRITO
</div>
""", unsafe_allow_html=True)


# ─── Session init + routing ───────────────────────────────────────
_init_session()

# ─── Navegação via query param (ícones HTML) ──────────────────────
_qnav = st.query_params.get("nav", "")
if _qnav and st.session_state.get("authenticated"):
    st.query_params.clear()
    st.session_state.prev_view = st.session_state.get("current_view", "portal")
    st.session_state.current_view = _qnav
    st.rerun()

if not st.session_state.authenticated:
    _show_login()
    st.stop()

# FAB -- CALCULADORA SAMBA  (components.html injector)
try:
    import streamlit.components.v1 as _stcomp
    _fab_path = ROOT / 'assets' / 'fab_injector.html'
    if _fab_path.exists():
        _stcomp.html(_fab_path.read_text(encoding='utf-8'), height=0, scrolling=False)
except Exception:
    pass

if st.session_state.current_view == "portal":
    _show_portal()
    st.stop()

# ─── Hub da Equipe (conhecimento corporativo) ────────────────────
if st.session_state.current_view == "hub":
    from dashboards.hub_conhecimento import render_hub_conhecimento
    render_hub_conhecimento()
    st.stop()

# ─── Comercial / Prospecção ───────────────────────────────────────
if st.session_state.current_view == "comercial":
    from dashboards.comercial_hub import render_comercial_hub
    render_comercial_hub()
    st.stop()

# ─── Gerador de Documentos (hub) ─────────────────────────────────
if st.session_state.current_view == "documentos":
    from dashboards.doc_hub import render_doc_hub
    render_doc_hub()
    st.stop()

# ─── LOI — Letter of Intent ──────────────────────────────────────
if st.session_state.current_view == "loi":
    from dashboards.loi_widget import render_loi_widget
    render_loi_widget()
    st.stop()

# ─── NCNDA — Non-Circumvention, Non-Disclosure Agreement ─────────
if st.session_state.current_view == "ncnda":
    from dashboards.ncnda_widget import render_ncnda_widget
    render_ncnda_widget()
    st.stop()

# ─── Control Desk: sidebar nav ───────────────────────────────────
with st.sidebar:
    st.markdown("""
<div style='font-family:Montserrat,sans-serif;font-size:10px;font-weight:700;
  letter-spacing:1.5px;color:#FA8200;padding:8px 0 4px'>NAVEGAÇÃO</div>
""", unsafe_allow_html=True)
    if st.button("← Portal", key="cd_back"):
        st.session_state.current_view = "portal"
        st.rerun()
    if st.button("Sair", key="cd_logout"):
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()
    st.markdown("---")
    st.markdown(f"""<div style='font-size:10px;color:#7F7F7F;font-family:Montserrat,sans-serif'>
{st.session_state.get('user_email','')}</div>""", unsafe_allow_html=True)
    # ─── FAB injector: dentro do sidebar, invisível ───────────────
    # Usa window.top para alcançar o documento raiz do Streamlit
    st.components.v1.html("""<script>
(function run(){
  var pd;
  try{pd=window.top.document;}catch(e){try{pd=window.parent.document;}catch(e2){return;}}
  if(!pd||!pd.body){setTimeout(run,150);return;}
  if(pd.getElementById('sc-fab'))return;
  if(!pd||!pd.body){setTimeout(run,100);return;}
  if(pd.getElementById('sc-fab'))return;

  // ── CSS no head pai ─────────────────────────────────────────────
  var css=pd.createElement('style');css.id='sc-css';
  css.textContent=
    '#sc-fab{position:fixed;bottom:28px;right:28px;z-index:2147483646;'+
    'width:60px;height:60px;border-radius:50%;background:#FA8200;'+
    'color:#fff;border:none;cursor:pointer;'+
    'box-shadow:0 6px 28px rgba(250,130,0,.65),0 2px 8px rgba(0,0,0,.4);'+
    'display:flex;align-items:center;justify-content:center;'+
    'transition:transform .2s,box-shadow .2s;padding:0}'+
    '#sc-fab:hover{transform:scale(1.12);box-shadow:0 10px 36px rgba(250,130,0,.8)}'+
    '#sc-fab svg{width:26px;height:26px;fill:none;stroke:#fff;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}'+
    '#sc-fab.open{background:#1a1a1a;border:2px solid #FA8200}'+
    '#sc-fab.open svg{stroke:#FA8200}'+
    '#sc-ov{position:fixed;inset:0;z-index:2147483645;background:rgba(0,0,0,.45);'+
    'display:none;align-items:center;justify-content:center;backdrop-filter:blur(3px)}'+
    '#sc-ov.show{display:flex}'+
    '#sc-box{background:#ffffff;border:1px solid #E8E9EC;border-radius:14px;'+
    'width:720px;max-width:95vw;max-height:90vh;overflow-y:auto;padding:24px 26px;'+
    'position:relative;font-family:Montserrat,"Segoe UI",sans-serif;'+
    'box-shadow:0 8px 40px rgba(0,0,0,.15)}'+
    '#sc-close{position:absolute;top:16px;right:18px;background:none;border:none;'+
    'color:#BFBFBF;font-size:22px;cursor:pointer;font-weight:300;line-height:1;padding:0}'+
    '#sc-close:hover{color:#1A1A1A}'+
    '.sc-h{font-size:9px;font-weight:700;letter-spacing:2.5px;color:#FA8200;'+
    'margin-bottom:18px;text-transform:uppercase;display:flex;align-items:center;gap:10px}'+
    '.sc-h::after{content:"";flex:1;height:1px;background:rgba(250,130,0,.2)}'+
    '.sc-tabs{display:flex;gap:2px;margin-bottom:16px;border-bottom:2px solid #E8E9EC}'+
    '.sc-tab{flex:1;padding:8px 4px;text-align:center;font-size:10px;font-weight:700;'+
    'letter-spacing:.8px;cursor:pointer;color:#BFBFBF;border-bottom:3px solid transparent;'+
    'background:transparent;transition:all .15s;border-radius:4px 4px 0 0}'+
    '.sc-tab.on{color:#FA8200;border-bottom-color:#FA8200;background:rgba(250,130,0,.05)}'+
    '.sc-tab:hover:not(.on){color:#7F7F7F;background:#F9F9FB}'+
    '.sc-grid{display:grid;grid-template-columns:1.5fr 1.4fr .85fr 1.05fr;gap:10px;margin-bottom:14px}'+
    '.sc-col{display:flex;flex-direction:column}'+
    '.sc-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}'+
    '.sc-lbl{font-size:9px;font-weight:700;letter-spacing:1.2px;color:#7F7F7F;text-transform:uppercase}'+
    '.sc-pill{display:inline-flex;background:#F4F5F7;border:1px solid #E0E0E0;border-radius:5px;overflow:hidden}'+
    '.sc-pill button{border:none;background:none;color:#7F7F7F;font-family:inherit;font-size:9px;font-weight:700;padding:3px 8px;cursor:pointer;transition:all .12s}'+
    '.sc-pill button.on{background:#FA8200;color:#fff}'+
    '.sc-inp{width:100%;background:#ffffff;border:1px solid #E0E0E0;border-radius:7px;'+
    'padding:9px 11px;color:#1A1A1A;font-family:inherit;font-size:14px;font-weight:600;outline:none;transition:border-color .15s}'+
    '.sc-inp:focus{border-color:#FA8200;box-shadow:0 0 0 3px rgba(250,130,0,.12)}'+
    '.sc-hint{font-size:9px;color:#FA8200;margin-top:4px;min-height:12px;font-weight:600;text-align:right}'+
    '.sc-div{height:1px;background:#E8E9EC;margin:0 0 14px}'+
    '.sc-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:14px}'+
    '.sc-card{background:#F9F9FB;border:1px solid #E8E9EC;border-radius:9px;padding:13px 14px;transition:border-color .15s;box-shadow:0 1px 3px rgba(0,0,0,.04)}'+
    '.sc-card:hover{border-color:rgba(250,130,0,.35)}'+
    '.sc-cl{font-size:9px;font-weight:700;letter-spacing:1px;color:#BFBFBF;text-transform:uppercase;margin-bottom:6px}'+
    '.sc-cv{font-size:15px;font-weight:800;color:#1A1A1A;line-height:1.1}'+
    '.sc-cv2{font-size:11px;font-weight:700;color:#7F7F7F;margin-top:3px}'+
    '.sc-cs{font-size:10px;color:#BFBFBF;margin-top:2px}'+
    '.sc-card.qty{border-color:rgba(250,130,0,.3)}.sc-card.qty .sc-cv{color:#FA8200}'+
    '.sc-card.pos{border-color:rgba(50,150,50,.3)}.sc-card.pos .sc-cv{color:#329632}'+
    '.sc-card.neg{border-color:rgba(220,50,40,.25)}.sc-card.neg .sc-cv{color:#D93025}'+
    '.sc-card.zer .sc-cv{color:#BFBFBF}'+
    '.sc-sum{background:#FFF8F0;border:1px solid rgba(250,130,0,.25);border-radius:8px;'+
    'padding:9px 14px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px}'+
    '.sc-si{font-size:10px;font-weight:700;color:#7F7F7F}.sc-si span{color:#FA8200}'+
    '.sc-sep{color:#E8E9EC}'+
    '.sc-abtn{width:100%;padding:9px;background:#FFF8F0;border:1px solid rgba(250,130,0,.3);'+
    'border-radius:8px;color:#FA8200;font-family:inherit;font-size:10px;font-weight:700;'+
    'letter-spacing:1px;cursor:pointer;margin-bottom:10px;transition:background .15s}'+
    '.sc-abtn:hover{background:rgba(250,130,0,.12)}'+
    '.sc-ap{background:#F9F9FB;border:1px solid #E8E9EC;border-radius:9px;'+
    'padding:11px;max-height:240px;overflow-y:auto;margin-bottom:8px}'+
    '.sc-ar{display:flex;align-items:center;gap:6px;padding:7px 8px;border-radius:7px;'+
    'margin-bottom:4px;background:#ffffff;border:1px solid #E8E9EC}'+
    '.sc-ar:hover{border-color:rgba(250,130,0,.3)}'+
    '.sc-at{font-size:9px;font-weight:700;border-radius:3px;padding:1px 6px;min-width:52px;text-align:center}'+
    '.sc-an{flex:1;font-size:11px;font-weight:600;color:#7F7F7F}'+
    '.sc-av{font-size:11px;font-weight:700;min-width:70px;text-align:right;color:#1A1A1A}'+
    '.sc-au{font-size:11px;font-weight:700;min-width:60px;text-align:right;color:#7F7F7F}'+
    '.sc-asp{font-size:11px;font-weight:800;min-width:84px;text-align:right}'+
    '.sc-apct{font-size:10px;font-weight:700;min-width:46px;text-align:right}'+
    '.sc-bw{width:42px;height:5px;background:#E8E9EC;border-radius:3px;overflow:hidden;margin-left:4px}'+
    '.sc-bi{height:100%;border-radius:3px}'+
    '.sc-foot{font-size:9px;color:#BFBFBF;padding-top:6px;border-top:1px solid #E8E9EC;margin-top:4px;text-align:center}';
  pd.head.appendChild(css);

  // ── HTML no body pai ────────────────────────────────────────────
  var root=pd.createElement('div');root.id='sc-root';
  root.innerHTML=
    '<button id="sc-fab" onclick="scTgl()" title="Calculadora de Commodities">'+
      '<svg viewBox="0 0 24 24"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>'+
    '</button>'+
    '<div id="sc-ov" onclick="if(event.target===this)scTgl()">'+
     '<div id="sc-box">'+
      '<button id="sc-close" onclick="scTgl()">&#x2715;</button>'+
      '<div class="sc-h">Calculadora de Commodities</div>'+
      '<div class="sc-tabs">'+
       '<div class="sc-tab on" data-c="soja" onclick="scComm(this)">SOJA</div>'+
       '<div class="sc-tab" data-c="milho" onclick="scComm(this)">MILHO</div>'+
       '<div class="sc-tab" data-c="cafe" onclick="scComm(this)">CAFÉ</div>'+
       '<div class="sc-tab" data-c="acucar" onclick="scComm(this)">AÇÚCAR</div>'+
       '<div class="sc-tab" data-c="trigo" onclick="scComm(this)">TRIGO</div>'+
      '</div>'+
      '<div class="sc-grid">'+
       '<div class="sc-col"><div class="sc-row"><span class="sc-lbl">Quantidade</span><div class="sc-pill" id="sc-qp"><button class="on" onclick="scQm(\'sc\',this)">SACAS</button><button onclick="scQm(\'mt\',this)">MT</button></div></div><input id="sc-iq" class="sc-inp" value="2.000.000" oninput="scCalc()"><div class="sc-hint" id="sc-eq">= &mdash; MT</div></div>'+
       '<div class="sc-col"><div class="sc-row"><span class="sc-lbl">Preço</span><div class="sc-pill" id="sc-pp"><button class="on" onclick="scPm(\'bs\',this)">R$/SC</button><button onclick="scPm(\'um\',this)">USD/MT</button><button onclick="scPm(\'bm\',this)">BRL/MT</button></div></div><input id="sc-ip" class="sc-inp" value="105,00" oninput="scCalc()"><div class="sc-hint" id="sc-ep">= &mdash; USD/MT</div></div>'+
       '<div class="sc-col"><span class="sc-lbl">Câmbio</span><input id="sc-ifx" class="sc-inp" value="5,75" oninput="scCalc()"><div class="sc-hint">R$ / USD</div></div>'+
       '<div class="sc-col"><span class="sc-lbl">Ref. Bolsa</span><input id="sc-ir" class="sc-inp" value="920,00" oninput="scCalc()"><div class="sc-hint" id="sc-er">¢/bu (CBOT)</div></div>'+
      '</div>'+
      '<div class="sc-div"></div>'+
      '<div class="sc-cards">'+
       '<div class="sc-card qty"><div class="sc-cl">Volume Total</div><div class="sc-cv" id="sc-r1">&mdash;</div><div class="sc-cv2" id="sc-r2">&mdash;</div><div class="sc-cs" id="sc-r3">&mdash;</div></div>'+
       '<div class="sc-card"><div class="sc-cl">Total BRL</div><div class="sc-cv" id="sc-r4">&mdash;</div><div class="sc-cv2" id="sc-r5">&mdash;</div><div class="sc-cs" id="sc-r6">&mdash;</div></div>'+
       '<div class="sc-card"><div class="sc-cl">Total USD</div><div class="sc-cv" id="sc-r7">&mdash;</div><div class="sc-cv2" id="sc-r8">&mdash;</div><div class="sc-cs" id="sc-r9">&mdash;</div></div>'+
       '<div class="sc-card"><div class="sc-cl">Preço Unitário</div><div class="sc-cv" id="sc-r10">&mdash;</div><div class="sc-cv2" id="sc-r11">&mdash;</div><div class="sc-cs" id="sc-r12">&mdash;</div></div>'+
       '<div class="sc-card"><div class="sc-cl">Ref. Bolsa USD/MT</div><div class="sc-cv" id="sc-r13">&mdash;</div><div class="sc-cv2" id="sc-r14">&mdash;</div><div class="sc-cs" id="sc-r15">&mdash;</div></div>'+
       '<div class="sc-card zer" id="sc-rsc"><div class="sc-cl">Spread vs. Bolsa</div><div class="sc-cv" id="sc-r16">&mdash;</div><div class="sc-cv2" id="sc-r17">&mdash;</div><div class="sc-cs" id="sc-r18">&mdash;</div></div>'+
      '</div>'+
      '<div class="sc-sum"><div class="sc-si">Vol: <span id="sc-sv">&mdash;</span></div><div class="sc-sep">&middot;</div><div class="sc-si">Total: <span id="sc-st">&mdash;</span></div><div class="sc-sep">&middot;</div><div class="sc-si">Spread: <span id="sc-ss">&mdash;</span></div><div class="sc-sep">&middot;</div><div class="sc-si">FX: <span id="sc-sfx">&mdash;</span></div></div>'+
      '<button class="sc-abtn" id="sc-abtn" onclick="scArb()">⇄ ARBITRAGEM COMPARATIVA POR PRAÇA</button>'+
      '<div id="sc-ap" style="display:none" class="sc-ap"><div id="sc-ab"></div><div class="sc-foot">Referências de praça para simulação · Atualizar com feed ao vivo</div></div>'+
      '<div class="sc-foot">Ref. CBOT em ¢/bushel · ICE em ¢/lb · Spread = operação − referência bolsa em USD/MT</div>'+
     '</div>'+
    '</div>';
  pd.body.appendChild(root);

  // ── Script no contexto pai (funções referenciam document do pai) ──
  var sc=pd.createElement('script');sc.id='sc-js';
  sc.textContent=
    'var scC={soja:{kg:60,u:"bushel",ex:"CBOT",bk:27.2155,r:920},milho:{kg:60,u:"bushel",ex:"CBOT",bk:25.4012,r:415},cafe:{kg:60,u:"lb",ex:"ICE",bk:.453592,r:21000},acucar:{kg:50,u:"lb",ex:"ICE",bk:.453592,r:1950},trigo:{kg:60,u:"bushel",ex:"CBOT",bk:27.2155,r:530}};'+
    'var scP={soja:[{n:"Santos FOB",t:"P",b:118.5},{n:"Paraná FOB",t:"P",b:117.2},{n:"Mato Grosso",t:"I",b:101.8},{n:"Goiás",t:"I",b:104.3},{n:"Paraná",t:"I",b:109.6},{n:"Rio Grande",t:"I",b:111.4},{n:"Rotterdam",t:"X",u:345},{n:"Shanghai",t:"X",u:352}],milho:[{n:"Santos FOB",t:"P",b:62.8},{n:"Paraná FOB",t:"P",b:61.9},{n:"Mato Grosso",t:"I",b:54.2},{n:"Goiás",t:"I",b:56.1},{n:"Paraná",t:"I",b:58.4},{n:"Rotterdam",t:"X",u:168}],cafe:[{n:"Santos ESALQ",t:"P",b:1420},{n:"Minas Gerais",t:"I",b:1380},{n:"São Paulo",t:"I",b:1395},{n:"Bahia",t:"I",b:1360},{n:"NY ICE",t:"X",u:5210},{n:"Londres",t:"X",u:4980}],acucar:[{n:"Santos VHP FOB",t:"P",b:138},{n:"São Paulo",t:"I",b:128},{n:"Paraná",t:"I",b:126},{n:"NY ICE No.11",t:"X",u:441},{n:"Londres ICE W.",t:"X",u:460}],trigo:[{n:"Paraná FOB",t:"P",b:78.5},{n:"Paraná",t:"I",b:71.2},{n:"Rio Grande",t:"I",b:72.8},{n:"Mato Grosso",t:"I",b:68.4},{n:"Rotterdam",t:"X",u:205}]};'+
    'var scTC={P:"#64C8FA",I:"#BFBFBF",X:"#FA8200"},scTL={P:"PORTO",I:"INTERIOR",X:"INTL"};'+
    'var scCm="soja",scQmode="sc",scPmode="bs",scAo=false;'+
    'function scN(s){return parseFloat((s||"").toString().replace(/\\./g,"").replace(",","."))||0;}'+
    'function scFn(n,d){return n.toLocaleString("pt-BR",{minimumFractionDigits:d,maximumFractionDigits:d});}'+
    'function scFb(n){return n>=1e9?"R$ "+scFn(n/1e9,2)+"bi":n>=1e6?"R$ "+scFn(n/1e6,2)+"mi":"R$ "+scFn(n,2);}'+
    'function scFu(n){return n>=1e9?"$ "+scFn(n/1e9,2)+"bi":n>=1e6?"$ "+scFn(n/1e6,2)+"mi":"$ "+scFn(n,2);}'+
    'function scS(id,v){var e=document.getElementById(id);if(e)e.textContent=v;}'+
    'function scTgl(){var ov=document.getElementById("sc-ov");ov.classList.toggle("show");document.getElementById("sc-fab").classList.toggle("open");}'+
    'function scComm(el){document.querySelectorAll(".sc-tab").forEach(function(t){t.classList.remove("on")});el.classList.add("on");scCm=el.dataset.c;document.getElementById("sc-ir").value=scFn(scC[scCm].r,2);document.getElementById("sc-er").textContent="¢/"+scC[scCm].u+" ("+scC[scCm].ex+")";if(scAo)scRa();scCalc();}'+
    'function scQm(m,b){scQmode=m;document.querySelectorAll("#sc-qp button").forEach(function(x){x.classList.remove("on")});b.classList.add("on");document.getElementById("sc-iq").value=m==="sc"?"2.000.000":"120.000";scCalc();}'+
    'function scPm(m,b){scPmode=m;document.querySelectorAll("#sc-pp button").forEach(function(x){x.classList.remove("on")});b.classList.add("on");scCalc();}'+
    'function scUmt(){var c=scC[scCm],pr=scN(document.getElementById("sc-ip").value),fx=scN(document.getElementById("sc-ifx").value);if(scPmode==="bs")return(pr*1000/c.kg)/fx;if(scPmode==="um")return pr;return pr/fx;}'+
    'function scCalc(){'+
      'var c=scC[scCm],raw=scN(document.getElementById("sc-iq").value),pr=scN(document.getElementById("sc-ip").value),fx=scN(document.getElementById("sc-ifx").value),ref=scN(document.getElementById("sc-ir").value);'+
      'if(!raw||!pr||!fx||!ref)return;'+
      'var mt,sc2;if(scQmode==="sc"){sc2=raw;mt=raw*c.kg/1000;}else{mt=raw;sc2=raw*1000/c.kg;}'+
      'var um,bm,bs2;if(scPmode==="bs"){bs2=pr;bm=pr*1000/c.kg;um=bm/fx;}else if(scPmode==="um"){um=pr;bm=pr*fx;bs2=bm*c.kg/1000;}else{bm=pr;um=pr/fx;bs2=bm*c.kg/1000;}'+
      'var tb=bm*mt,tu=um*mt,ru=(ref/100)*(1000/c.bk),rb=ru*fx,rbs=rb*c.kg/1000,sp=um-ru,spP=ru>0?(sp/ru)*100:0,spB=sp*fx,sg=sp>=0?"+":"";'+
      'if(scQmode==="sc"){scS("sc-r1",scFn(sc2,0)+" sacas");scS("sc-r2",scFn(mt,2)+" MT");scS("sc-eq","= "+scFn(mt,2)+" MT");}else{scS("sc-r1",scFn(mt,2)+" MT");scS("sc-r2",scFn(sc2,0)+" sacas");scS("sc-eq","= "+scFn(sc2,0)+" sacas");}'+
      'scS("sc-r3",scFn(mt*1000,0)+" kg total");'+
      'if(scPmode==="bs")scS("sc-ep","= $ "+scFn(um,2)+"/MT");else if(scPmode==="um")scS("sc-ep","= R$ "+scFn(bs2,2)+"/saca");else scS("sc-ep","= $ "+scFn(um,2)+"/MT");'+
      'scS("sc-r4",scFb(tb));scS("sc-r5","R$ "+scFn(bs2,2)+"/saca");scS("sc-r6","R$ "+scFn(bm,2)+"/MT");'+
      'scS("sc-r7",scFu(tu));scS("sc-r8","$ "+scFn(um,2)+"/MT");scS("sc-r9",scFn(tu,0)+" USD");'+
      'scS("sc-r10","$ "+scFn(um,2)+"/MT");scS("sc-r11","R$ "+scFn(bm,2)+"/MT");scS("sc-r12","R$ "+scFn(bs2,2)+"/saca");'+
      'scS("sc-r13","$ "+scFn(ru,2)+"/MT");scS("sc-r14","R$ "+scFn(rbs,2)+"/saca");scS("sc-r15",c.ex+" "+scFn(ref,2)+" ¢/"+c.u);'+
      'scS("sc-r16",sg+"$ "+scFn(Math.abs(sp),2)+"/MT "+(sp>=0?"▲":"▼"));scS("sc-r17",sg+"R$ "+scFn(Math.abs(spB),2)+"/MT");scS("sc-r18",sg+scFn(spP,2)+"% vs. "+c.ex);'+
      'var rsc=document.getElementById("sc-rsc");rsc.className="sc-card "+(sp>0.5?"pos":sp<-0.5?"neg":"zer");'+
      'scS("sc-sv",scFn(mt,0)+" MT / "+scFn(sc2,0)+" sacas");scS("sc-st",scFb(tb)+" · "+scFu(tu));scS("sc-ss",sg+"$ "+scFn(Math.abs(sp),2)+"/MT ("+sg+scFn(spP,2)+"%)");scS("sc-sfx","R$ "+scFn(fx,4)+"/USD");'+
      'if(scAo)scRa();'+
    '}'+
    'function scArb(){scAo=!scAo;document.getElementById("sc-ap").style.display=scAo?"block":"none";document.getElementById("sc-abtn").textContent=scAo?"▲ FECHAR ARBITRAGEM":"⇄ ARBITRAGEM COMPARATIVA POR PRAÇA";if(scAo)scRa();}'+
    'function scRa(){'+
      'var c=scC[scCm],fx=scN(document.getElementById("sc-ifx").value),um=scUmt(),bs=um*fx*c.kg/1000;'+
      'var h=\'<div class="sc-ar" style="background:rgba(250,130,0,.08);border-color:rgba(250,130,0,.35)"><span class="sc-at" style="background:#FA820022;color:#FA8200;border:1px solid #FA820044">SUA OP.</span><span class="sc-an" style="color:#FA8200;font-weight:700">Preço inserido</span><span class="sc-av" style="color:#FA8200">R$ \'+scFn(bs,2)+\'</span><span class="sc-au" style="color:#FA8200">$\'+scFn(um,2)+\'</span><span class="sc-asp" style="color:#7f7f7f">&mdash;</span><span class="sc-apct" style="color:#7f7f7f">&mdash;</span><div class="sc-bw"></div></div>\';'+
      '(scP[scCm]||[]).forEach(function(p){var pu=p.u||(p.b*1000/c.kg)/fx,pb=p.b||(p.u*fx*c.kg/1000),sp=um-pu,spP=pu>0?(sp/pu)*100:0,sg=sp>=0?"+":"",col=sp>1?"#32b432":sp<-1?"#fa3232":"#BFBFBF",tc=scTC[p.t]||"#888",tl=scTL[p.t]||p.t,bw=Math.min(Math.abs(spP)*3.5,42),bd=sp>=0?"margin-left:auto":"";'+
        'h+=\'<div class="sc-ar"><span class="sc-at" style="background:\'+tc+\'22;color:\'+tc+\';border:1px solid \'+tc+\'44">\'+tl+\'</span><span class="sc-an">\'+p.n+\'</span><span class="sc-av">R$ \'+scFn(pb,2)+\'</span><span class="sc-au">$\'+scFn(pu,2)+\'</span><span class="sc-asp" style="color:\'+col+\'">\'+sg+\'$\'+scFn(Math.abs(sp),2)+\'/MT</span><span class="sc-apct" style="color:\'+col+\'">\'+sg+scFn(spP,1)+\'%</span><div class="sc-bw"><div class="sc-bi" style="width:\'+bw+\'px;background:\'+col+\';\'+bd+\'"></div></div></div>\';'+
      '});'+
      'document.getElementById("sc-ab").innerHTML=h;'+
    '}'+
    'scCalc();'+
    'document.addEventListener("keydown",function(e){if(e.key==="Escape"){var ov=document.getElementById("sc-ov");if(ov&&ov.classList.contains("show"))scTgl();}});';
  pd.body.appendChild(sc);
  setTimeout(function(){if(!pd.getElementById('sc-fab'))run();},1000);
})();
</script>""", height=2, scrolling=False)

# placeholder para o if antigo que foi removido
if False:
    st.components.v1.html("""<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Montserrat','Segoe UI',sans-serif;background:#111;color:#F5F5F5;padding:18px 20px}
:root{--so:#FA8200;--sb50:#7F7F7F;--sb25:#BFBFBF;--red:#FA3232;--green:#329632;--line:rgba(255,255,255,.07)}
h2{font-size:13px;font-weight:800;letter-spacing:2px;color:var(--so);margin-bottom:16px;text-transform:uppercase;border-bottom:2px solid var(--so);padding-bottom:8px}
.ctabs{display:flex;border-bottom:2px solid rgba(250,130,0,.3);margin-bottom:18px;gap:2px}
.ctab{flex:1;padding:10px 4px;text-align:center;font-size:11px;font-weight:700;letter-spacing:1px;cursor:pointer;color:var(--sb50);border-bottom:3px solid transparent;background:#111;transition:all .15s;border-radius:4px 4px 0 0}
.ctab.on{color:var(--so);border-bottom-color:var(--so);background:rgba(250,130,0,.07)}
.ctab:hover:not(.on){color:var(--sb25);background:rgba(255,255,255,.03)}
.ig{display:grid;grid-template-columns:1.5fr 1.4fr .85fr 1.05fr;gap:11px;margin-bottom:14px}
.fl{display:flex;flex-direction:column}
.fh{display:flex;align-items:center;justify-content:space-between;margin-bottom:5px}
label{font-size:9px;font-weight:700;letter-spacing:1.2px;color:var(--sb50);text-transform:uppercase}
.pill{display:inline-flex;background:#0a0a0a;border:1px solid rgba(255,255,255,.1);border-radius:5px;overflow:hidden}
.pill button{border:none;background:none;color:var(--sb50);font-family:inherit;font-size:9px;font-weight:700;letter-spacing:.6px;padding:3px 8px;cursor:pointer;transition:all .12s}
.pill button.on{background:var(--so);color:#fff}
input{width:100%;background:#0a0a0a;border:1px solid rgba(255,255,255,.1);border-radius:7px;padding:9px 11px;color:#fff;font-family:inherit;font-size:14px;font-weight:600;outline:none;transition:border-color .15s}
input:focus{border-color:var(--so);box-shadow:0 0 0 2px rgba(250,130,0,.12)}
.eq{font-size:9px;color:rgba(250,130,0,.7);margin-top:4px;min-height:13px;font-weight:600;text-align:right}
.dv{height:1px;background:var(--line);margin:0 0 14px}
.rg{display:grid;grid-template-columns:repeat(3,1fr);gap:9px;margin-bottom:14px}
.card{background:#0d0d0d;border:1px solid rgba(255,255,255,.07);border-radius:9px;padding:13px 14px;transition:border-color .15s}
.card:hover{border-color:rgba(250,130,0,.28)}
.cl{font-size:9px;font-weight:700;letter-spacing:1px;color:var(--sb50);text-transform:uppercase;margin-bottom:7px}
.cv{font-size:15px;font-weight:800;color:#fff;line-height:1.1}
.cv2{font-size:11px;font-weight:700;color:var(--sb25);margin-top:3px}
.cs{font-size:10px;color:var(--sb50);margin-top:2px}
.card.qty{border-color:rgba(250,130,0,.25)}.card.qty .cv{color:var(--so)}
.card.pos{border-color:rgba(50,150,50,.35)}.card.pos .cv{color:var(--green)}
.card.neg{border-color:rgba(250,50,50,.28)}.card.neg .cv{color:var(--red)}
.card.zer .cv{color:var(--sb25)}
.sb{background:rgba(250,130,0,.07);border:1px solid rgba(250,130,0,.2);border-radius:8px;padding:9px 13px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:11px}
.si{font-size:10px;font-weight:700;color:var(--sb25)}.si span{color:var(--so)}
.sep{color:rgba(255,255,255,.1)}
.abtn{width:100%;padding:9px;background:rgba(250,130,0,.07);border:1px solid rgba(250,130,0,.25);border-radius:8px;color:var(--so);font-family:inherit;font-size:10px;font-weight:700;letter-spacing:1px;cursor:pointer;margin-bottom:10px;transition:background .15s}
.abtn:hover{background:rgba(250,130,0,.15)}
.ap{background:#0a0a0a;border:1px solid var(--line);border-radius:9px;padding:11px;max-height:280px;overflow-y:auto;margin-bottom:10px}
.ar{display:flex;align-items:center;gap:6px;padding:7px 8px;border-radius:7px;margin-bottom:4px;background:#111;border:1px solid rgba(255,255,255,.06)}
.ar:hover{border-color:rgba(250,130,0,.22)}
.at{font-size:9px;font-weight:700;border-radius:3px;padding:1px 6px;min-width:56px;text-align:center}
.an{flex:1;font-size:11px;font-weight:600;color:var(--sb25)}
.av,.au{font-size:11px;font-weight:700;min-width:74px;text-align:right}
.av{color:#fff}.au{color:var(--sb25);min-width:62px}
.asp{font-size:11px;font-weight:800;min-width:88px;text-align:right}
.apct{font-size:10px;font-weight:700;min-width:52px;text-align:right}
.bw{width:48px;height:6px;background:#1a1a1a;border-radius:3px;overflow:hidden;margin-left:4px}
.bi{height:100%;border-radius:3px}
.fn{font-size:9px;color:var(--sb50);padding-top:6px;border-top:1px solid var(--line);margin-top:4px;text-align:center}
</style></head><body>
<h2>Calculadora de Commodities</h2>
<div class="ctabs">
  <div class="ctab on" data-c="soja" onclick="sc(this)">SOJA</div>
  <div class="ctab" data-c="milho" onclick="sc(this)">MILHO</div>
  <div class="ctab" data-c="cafe" onclick="sc(this)">CAFÉ</div>
  <div class="ctab" data-c="acucar" onclick="sc(this)">AÇÚCAR</div>
  <div class="ctab" data-c="trigo" onclick="sc(this)">TRIGO</div>
</div>
<div class="ig">
  <div class="fl"><div class="fh"><label>Quantidade</label><div class="pill" id="qp"><button class="on" onclick="sqm('sc',this)">SACAS</button><button onclick="sqm('mt',this)">MT</button></div></div><input id="iq" value="2.000.000" oninput="cc()"><div class="eq" id="eq">= — MT</div></div>
  <div class="fl"><div class="fh"><label>Preço</label><div class="pill" id="pp"><button class="on" onclick="spm('bs',this)">R$/SC</button><button onclick="spm('um',this)">USD/MT</button><button onclick="spm('bm',this)">BRL/MT</button></div></div><input id="ip" value="105,00" oninput="cc()"><div class="eq" id="ep">= — USD/MT</div></div>
  <div class="fl"><label>Câmbio</label><input id="ifx" value="5,75" oninput="cc()"><div class="eq">R$ / USD</div></div>
  <div class="fl"><label>Ref. Bolsa</label><input id="ir" value="920,00" oninput="cc()"><div class="eq" id="er">¢/bu (CBOT)</div></div>
</div>
<div class="dv"></div>
<div class="rg">
  <div class="card qty"><div class="cl">Volume Total</div><div class="cv" id="r1">—</div><div class="cv2" id="r2">—</div><div class="cs" id="r3">—</div></div>
  <div class="card"><div class="cl">Total BRL</div><div class="cv" id="r4">—</div><div class="cv2" id="r5">—</div><div class="cs" id="r6">—</div></div>
  <div class="card"><div class="cl">Total USD</div><div class="cv" id="r7">—</div><div class="cv2" id="r8">—</div><div class="cs" id="r9">—</div></div>
  <div class="card"><div class="cl">Preço Unitário</div><div class="cv" id="r10">—</div><div class="cv2" id="r11">—</div><div class="cs" id="r12">—</div></div>
  <div class="card"><div class="cl">Ref. Bolsa USD/MT</div><div class="cv" id="r13">—</div><div class="cv2" id="r14">—</div><div class="cs" id="r15">—</div></div>
  <div class="card zer" id="rsc"><div class="cl">Spread vs. Bolsa</div><div class="cv" id="r16">—</div><div class="cv2" id="r17">—</div><div class="cs" id="r18">—</div></div>
</div>
<div class="sb"><div class="si">Volume: <span id="sv">—</span></div><div class="sep">·</div><div class="si">Total: <span id="st">—</span></div><div class="sep">·</div><div class="si">Spread: <span id="ss">—</span></div><div class="sep">·</div><div class="si">Câmbio: <span id="sfx">—</span></div></div>
<button class="abtn" onclick="ta()">⇄ ARBITRAGEM COMPARATIVA POR PRAÇA</button>
<div id="ap" style="display:none" class="ap"><div id="ab"></div><div class="fn">Referências de praça para simulação · Atualizar com feed ao vivo</div></div>
<div class="fn">Ref. CBOT em ¢/bushel · ICE em ¢/lb · Spread = preço operação − referência bolsa em USD/MT</div>
<script>
const C={soja:{kg:60,u:'bushel',ex:'CBOT',bk:27.2155,r:920},milho:{kg:60,u:'bushel',ex:'CBOT',bk:25.4012,r:415},cafe:{kg:60,u:'lb',ex:'ICE',bk:.453592,r:21000},acucar:{kg:50,u:'lb',ex:'ICE',bk:.453592,r:1950},trigo:{kg:60,u:'bushel',ex:'CBOT',bk:27.2155,r:530}};
const P={soja:[{n:'Santos FOB',t:'P',b:118.5},{n:'Paranaguá FOB',t:'P',b:117.2},{n:'Mato Grosso',t:'I',b:101.8},{n:'Goiás',t:'I',b:104.3},{n:'Paraná',t:'I',b:109.6},{n:'Rio Grande',t:'I',b:111.4},{n:'Rotterdam',t:'X',u:345},{n:'Shanghai',t:'X',u:352}],milho:[{n:'Santos FOB',t:'P',b:62.8},{n:'Paranaguá FOB',t:'P',b:61.9},{n:'Mato Grosso',t:'I',b:54.2},{n:'Goiás',t:'I',b:56.1},{n:'Paraná',t:'I',b:58.4},{n:'Rotterdam',t:'X',u:168}],cafe:[{n:'Santos ESALQ',t:'P',b:1420},{n:'Minas Gerais',t:'I',b:1380},{n:'São Paulo',t:'I',b:1395},{n:'Bahia',t:'I',b:1360},{n:'NY ICE',t:'X',u:5210},{n:'Londres',t:'X',u:4980}],acucar:[{n:'Santos VHP FOB',t:'P',b:138},{n:'São Paulo',t:'I',b:128},{n:'Paraná',t:'I',b:126},{n:'NY ICE No.11',t:'X',u:441},{n:'Londres ICE W.',t:'X',u:460}],trigo:[{n:'Paranaguá FOB',t:'P',b:78.5},{n:'Paraná',t:'I',b:71.2},{n:'Rio Grande',t:'I',b:72.8},{n:'Mato Grosso',t:'I',b:68.4},{n:'Rotterdam',t:'X',u:205}]};
const TC={P:'#64C8FA',I:'#BFBFBF',X:'#FA8200'},TL={P:'PORTO',I:'INTERIOR',X:'INTL'};
let cm='soja',qm='sc',pm='bs',ao=false;
const pN=s=>parseFloat((s||'').toString().replace(/\./g,'').replace(',','.'))||0;
const fN=(n,d=2)=>n.toLocaleString('pt-BR',{minimumFractionDigits:d,maximumFractionDigits:d});
const fB=n=>n>=1e9?'R$ '+fN(n/1e9)+'bi':n>=1e6?'R$ '+fN(n/1e6)+'mi':'R$ '+fN(n);
const fU=n=>n>=1e9?'$ '+fN(n/1e9)+'bi':n>=1e6?'$ '+fN(n/1e6)+'mi':'$ '+fN(n);
const $=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v};
function sc(el){document.querySelectorAll('.ctab').forEach(t=>t.classList.remove('on'));el.classList.add('on');cm=el.dataset.c;document.getElementById('ir').value=fN(C[cm].r);document.getElementById('er').textContent='¢/'+C[cm].u+' ('+C[cm].ex+')';if(ao)ra();cc();}
function sqm(m,b){qm=m;document.querySelectorAll('#qp button').forEach(x=>x.classList.remove('on'));b.classList.add('on');document.getElementById('iq').value=m==='sc'?'2.000.000':'120.000';cc();}
function spm(m,b){pm=m;document.querySelectorAll('#pp button').forEach(x=>x.classList.remove('on'));b.classList.add('on');cc();}
function umt(){const c=C[cm],pr=pN(document.getElementById('ip').value),fx=pN(document.getElementById('ifx').value);if(pm==='bs')return(pr*1000/c.kg)/fx;if(pm==='um')return pr;return pr/fx;}
function cc(){
  const c=C[cm],raw=pN(document.getElementById('iq').value),pr=pN(document.getElementById('ip').value),fx=pN(document.getElementById('ifx').value),ref=pN(document.getElementById('ir').value);
  if(!raw||!pr||!fx||!ref)return;
  let mt,sc2;if(qm==='sc'){sc2=raw;mt=raw*c.kg/1000;}else{mt=raw;sc2=raw*1000/c.kg;}
  let um,bm,bs2;if(pm==='bs'){bs2=pr;bm=pr*1000/c.kg;um=bm/fx;}else if(pm==='um'){um=pr;bm=pr*fx;bs2=bm*c.kg/1000;}else{bm=pr;um=pr/fx;bs2=bm*c.kg/1000;}
  const tb=bm*mt,tu=um*mt,ru=(ref/100)*(1000/c.bk),rb=ru*fx,rbs=rb*c.kg/1000,sp=um-ru,spP=ru>0?(sp/ru)*100:0,spB=sp*fx,sg=sp>=0?'+':'';
  if(qm==='sc'){$('r1',fN(sc2,0)+' sacas');$('r2',fN(mt,2)+' MT');$('eq','= '+fN(mt,2)+' MT');}else{$('r1',fN(mt,2)+' MT');$('r2',fN(sc2,0)+' sacas');$('eq','= '+fN(sc2,0)+' sacas');}
  $('r3',fN(mt*1000,0)+' kg total');
  if(pm==='bs')$('ep','= $ '+fN(um,2)+'/MT');else if(pm==='um')$('ep','= R$ '+fN(bs2,2)+'/saca');else $('ep','= $ '+fN(um,2)+'/MT');
  $('r4',fB(tb));$('r5','R$ '+fN(bs2,2)+'/saca');$('r6','R$ '+fN(bm,2)+'/MT');
  $('r7',fU(tu));$('r8','$ '+fN(um,2)+'/MT');$('r9',fN(tu,0)+' USD');
  $('r10','$ '+fN(um,2)+'/MT');$('r11','R$ '+fN(bm,2)+'/MT');$('r12','R$ '+fN(bs2,2)+'/saca');
  $('r13','$ '+fN(ru,2)+'/MT');$('r14','R$ '+fN(rbs,2)+'/saca');$('r15',c.ex+' '+fN(ref,2)+' ¢/'+c.u);
  $('r16',sg+'$ '+fN(Math.abs(sp),2)+'/MT '+(sp>=0?'▲':'▼'));$('r17',sg+'R$ '+fN(Math.abs(spB),2)+'/MT');$('r18',sg+fN(spP,2)+'% vs. '+c.ex);
  const sc3=document.getElementById('rsc');sc3.className='card '+(sp>0.5?'pos':sp<-0.5?'neg':'zer');
  $('sv',fN(mt,0)+' MT / '+fN(sc2,0)+' sacas');$('st',fB(tb)+' · '+fU(tu));$('ss',sg+'$ '+fN(Math.abs(sp),2)+'/MT ('+sg+fN(spP,2)+'%)');$('sfx','R$ '+fN(fx,4)+'/USD');
  if(ao)ra();
}
function ta(){ao=!ao;document.getElementById('ap').style.display=ao?'block':'none';document.querySelector('.abtn').textContent=ao?'▲ FECHAR ARBITRAGEM':'⇄ ARBITRAGEM COMPARATIVA POR PRAÇA';if(ao)ra();}
function ra(){
  const c=C[cm],fx=pN(document.getElementById('ifx').value),um=umt(),bs=um*fx*c.kg/1000;
  let h=`<div class="ar" style="background:rgba(250,130,0,.08);border-color:rgba(250,130,0,.35)"><span class="at" style="background:#FA820022;color:#FA8200;border:1px solid #FA820044">SUA OP.</span><span class="an" style="color:#FA8200;font-weight:700">Preço inserido</span><span class="av" style="color:#FA8200">R$ ${fN(bs,2)}</span><span class="au" style="color:#FA8200">$${fN(um,2)}</span><span class="asp" style="color:var(--sb50)">—</span><span class="apct" style="color:var(--sb50)">—</span><div class="bw"></div></div>`;
  (P[cm]||[]).forEach(p=>{
    const pu=p.u||(p.b*1000/c.kg)/fx,pb=p.b||(p.u*fx*c.kg/1000),sp=um-pu,spP=pu>0?(sp/pu)*100:0,sg=sp>=0?'+':'',col=sp>1?'#329632':sp<-1?'#FA3232':'#BFBFBF',tc=TC[p.t]||'#888',tl=TL[p.t]||p.t,bw=Math.min(Math.abs(spP)*3.5,48),bd=sp>=0?'margin-left:auto':'';
    h+=`<div class="ar"><span class="at" style="background:${tc}22;color:${tc};border:1px solid ${tc}44">${tl}</span><span class="an">${p.n}</span><span class="av">R$ ${fN(pb,2)}</span><span class="au">$${fN(pu,2)}</span><span class="asp" style="color:${col}">${sg}$${fN(Math.abs(sp),2)}/MT</span><span class="apct" style="color:${col}">${sg}${fN(spP,1)}%</span><div class="bw"><div class="bi" style="width:${bw}px;background:${col};${bd}"></div></div></div>`;
  });
  document.getElementById('ab').innerHTML=h;
}
cc();
</script></body></html>""", height=860, scrolling=True)
    st.stop()

# ========================
# AUTO REFRESH
# ========================
try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=60000, key="refresh")
except Exception:
    if st.button("🔄 Atualizar"):
        st.rerun()

# ========================
# CSS GLOBAL — LIGHT MODE
# ========================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800;900&display=swap');

/* === SAMBA PREMIUM LIGHT — Manual de Marca 2026 v2 ======================== */
:root {
    --samba-bg:       #EDEAE4;
    --samba-bg-soft:  #F4F1EB;
    --samba-bg-card:  #ffffff;
    --samba-ice:      #1A1A1A;
    --samba-dim:      #6B7280;
    --samba-muted:    #BFBFBF;
    --samba-gold:     #FA8200;
    --samba-gold-dim: #C86600;
    --samba-line:     #DDD9D1;
    --samba-line-soft:#EAE7E1;
}

html, body, [class*="css"] {
    font-family: 'Montserrat', sans-serif !important;
    color: var(--samba-ice) !important;
}
.stApp {
    background: #EDEAE4 !important;
    color: var(--samba-ice);
}
header { visibility: hidden; }
.block-container { padding-top: 1rem; max-width: 1400px; }

/* ── Tabs principais ─────────────────────────────────────────────────────── */
[data-testid="stTabBar"] {
    background: #1A1C24 !important;
    border-bottom: none !important;
    border-radius: 10px !important;
    padding: 4px 6px !important;
    gap: 2px !important;
    margin-bottom: 12px !important;
}
button[data-baseweb="tab"] {
    color: #6B7280 !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 12px !important; font-weight: 700 !important;
    background: transparent !important;
    border-radius: 7px !important;
    padding: 7px 14px !important;
    letter-spacing: .6px !important;
    border-bottom: none !important;
    transition: all .15s !important;
}
button[data-baseweb="tab"][aria-selected="true"] {
    color: #ffffff !important;
    background: #FA8200 !important;
    border-bottom: none !important;
}
button[data-baseweb="tab"]:hover:not([aria-selected="true"]) {
    color: #D1D5DB !important;
    background: rgba(255,255,255,.07) !important;
}

/* ── Labels de selectbox/input ───────────────────────────────────────────── */
.stSelectbox label, .stTextInput label, .stNumberInput label {
    color: var(--samba-dim) !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 10px !important; font-weight: 700 !important;
    letter-spacing: 1.2px !important; text-transform: uppercase !important;
}

/* ── Selectbox ───────────────────────────────────────────────────────────── */
.stSelectbox [data-baseweb="select"] > div:first-child {
    background: #ffffff !important;
    border: 1px solid var(--samba-line) !important;
    border-radius: 8px !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 13px !important; font-weight: 600 !important;
    box-shadow: 0 1px 4px rgba(0,0,0,.06) !important;
    transition: border-color .15s, box-shadow .15s !important;
}
.stSelectbox [data-baseweb="select"]:focus-within > div:first-child {
    border-color: var(--samba-gold) !important;
    box-shadow: 0 0 0 3px rgba(250,130,0,.12) !important;
}

/* ── Number input ────────────────────────────────────────────────────────── */
.stNumberInput input {
    background: #ffffff !important;
    border: 1px solid var(--samba-line) !important;
    border-radius: 8px !important;
    font-family: 'Montserrat', sans-serif !important;
    font-size: 14px !important; font-weight: 700 !important;
    color: var(--samba-ice) !important;
}
.stNumberInput input:focus {
    border-color: var(--samba-gold) !important;
    box-shadow: 0 0 0 3px rgba(250,130,0,.12) !important;
}
.stNumberInput button {
    background: #F4F1EB !important;
    border: 1px solid var(--samba-line) !important;
    color: var(--samba-dim) !important;
    border-radius: 6px !important;
}
.stNumberInput button:hover { background: #FA8200 !important; color: #fff !important; border-color: #FA8200 !important; }

/* ── Botões nativos Streamlit ────────────────────────────────────────────── */
div.stButton > button {
    background: #1A1C24 !important;
    color: #D1D5DB !important;
    border: 1px solid #2E3141 !important;
    border-radius: 8px !important;
    font-family: 'Montserrat', sans-serif !important;
    font-weight: 700 !important; font-size: 12px !important;
    letter-spacing: .5px !important;
    box-shadow: 0 2px 6px rgba(0,0,0,.15) !important;
    transition: all .15s !important;
}
div.stButton > button:hover {
    background: #FA8200 !important;
    border-color: #FA8200 !important;
    color: #ffffff !important;
    box-shadow: 0 4px 14px rgba(250,130,0,.35) !important;
}
div.stButton > button:disabled {
    background: #2A2D38 !important;
    color: #4B5563 !important;
    border-color: #2E3141 !important;
    box-shadow: none !important;
}

/* ── FAB injector iframe — zero espaço visual ────────────────────────────── */
div[data-testid="stIFrame"] iframe { display:block; }
section[data-testid="stSidebar"] div[data-testid="stIFrame"] {
    height: 2px !important; min-height: 0 !important;
    overflow: hidden !important; opacity: 0 !important;
}

/* ── TICKER — alinhado com área de conteúdo dinâmico ─────────────────────── */
.ticker-wrap {
    width: 100%; overflow: hidden; background-color: #111111;
    border-radius: 8px;
    border: 1px solid #1f1f1f;
    padding: 11px 0; white-space: nowrap;
    box-shadow: 0 2px 6px rgba(0,0,0,.35);
}
.ticker { display: inline-block; animation: marquee 55s linear infinite; }
.ticker-item {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 0 18px; font-size: 13px;
    color: #BFBFBF; font-family: 'Montserrat', sans-serif;
}
.ticker-item span:first-child { color: #BFBFBF !important; }
.ticker-item span:nth-child(2) { color: #FA8200 !important; font-weight: 700 !important; }
.t-up   { color: #5fd17f !important; font-weight: 700; }
.t-down { color: #ff6b6b !important; font-weight: 700; }
.t-ativo { color: #7F7F7F; }
@keyframes marquee { 0% { transform: translateX(0); } 100% { transform: translateX(-25%); } }

/* ── CARDS ───────────────────────────────────────────────────────────────── */
.samba-card {
    background-color: var(--samba-bg-card); border-radius: 12px;
    border: 1px solid var(--samba-line); padding: 22px; margin-bottom: 16px;
    box-shadow: 0 2px 12px rgba(0,0,0,.08);
}
.section-title {
    font-size: 9px; letter-spacing: 2.2px; font-weight: 700;
    color: var(--samba-gold); margin-bottom: 14px; text-transform: uppercase;
    font-family: 'Montserrat', sans-serif;
    display: flex; align-items: center; gap: 8px;
}
.section-title::before {
    content: ""; display: inline-block; width: 3px; height: 14px;
    background: var(--samba-gold); border-radius: 2px;
}

/* ── DARK HEADER — Brand Manual pág. 3 ──────────────────────────────────── */
/* Targets the first horizontal block that contains the logo image */
[data-testid="stHorizontalBlock"]:has([data-testid="stImage"]) {
    background: #000000 !important;
    border-bottom: 3px solid #FA8200 !important;
    padding: 12px 24px !important;
    border-radius: 0 !important;
    box-shadow: 0 2px 16px rgba(0,0,0,.40) !important;
}
/* Ícones de ação no header — dark */
.cdact-btn {
    background: #1A1A1A !important;
    border: 1px solid #333333 !important;
    color: #7F7F7F !important;
}
.cdact-btn:hover {
    border-color: #FA8200 !important;
    color: #FA8200 !important;
    background: #2A1A00 !important;
}
.st-key-cd_docs_nav button {
    background-color: #1A1A1A !important;
    border: 1px solid #333 !important;
    border-radius: 7px !important;
    /* ícone SVG branco — filter inverte cinza para branco */
    filter: invert(0.7) !important;
}
.st-key-cd_docs_nav button:hover {
    background-color: #2A1A00 !important;
    border-color: #FA8200 !important;
    filter: invert(0) sepia(1) saturate(5) hue-rotate(0deg) !important;
}

/* ── KPI — Hero metrics row ──────────────────────────────────────────────── */
.kpi-card {
    flex: 1;
    background: #FA8200;
    padding: 20px 22px;
    border-radius: 12px;
    border: 1px solid rgba(255,255,255,.15);
    box-shadow: 0 6px 20px rgba(250,130,0,.30), 0 1px 4px rgba(0,0,0,.15);
    transition: transform .15s, box-shadow .15s;
}
.kpi-card:hover { transform: translateY(-3px); box-shadow: 0 10px 28px rgba(250,130,0,.40); }
.kpi-label { font-size: 9px; letter-spacing: 2px; color: rgba(255,255,255,0.75); font-weight: 700; text-transform: uppercase; }
.kpi-value { font-size: 30px; font-weight: 900; color: #FFFFFF; margin: 6px 0 2px; font-family: 'Montserrat', sans-serif; line-height: 1.05; text-shadow: 0 1px 4px rgba(0,0,0,.2); }
.kpi-sub   { font-size: 11px; color: rgba(255,255,255,0.85); font-weight: 600; }

/* ── RISK BADGES ─────────────────────────────────────────────────────────── */
.badge-baixo  { background:#E8F8EE; color:#329632; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:700; border:1px solid rgba(50,150,50,.2); }
.badge-medio  { background:#FFF4E0; color:#C86600; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:700; border:1px solid rgba(250,130,0,.25); }
.badge-alto   { background:#FFF0E8; color:#D95200; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:700; border:1px solid rgba(220,80,0,.25); }
.badge-critico{ background:#FEECEC; color:#D93025; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:700; border:1px solid rgba(220,50,40,.25); }

/* ── DOC BADGES ──────────────────────────────────────────────────────────── */
.doc-badge {
    display: inline-block; background: rgba(250,130,0,.10);
    color: var(--samba-gold); border: 1px solid rgba(250,130,0,.30);
    padding: 1px 7px; border-radius: 10px; font-size: 10px;
    font-weight: 700; margin: 0 4px 2px 0; letter-spacing: 0.5px;
}
.doc-badge-muted {
    display: inline-block; background: #F4F5F7;
    color: var(--samba-dim); border: 1px solid var(--samba-line);
    padding: 1px 7px; border-radius: 10px; font-size: 10px;
    font-weight: 600; margin: 0 4px 2px 0; letter-spacing: 0.5px;
}

/* ── KANBAN ──────────────────────────────────────────────────────────────── */
.kanban-col {
    background: var(--samba-bg-soft); border-radius: 12px; padding: 14px;
    border: 1px solid var(--samba-line); min-height: 120px;
}
.kanban-title { font-size:9px; letter-spacing:2.2px; color:var(--samba-gold); font-weight:700; margin-bottom:10px; text-transform:uppercase; }
.kanban-card {
    background: var(--samba-bg-card); border-radius: 8px; padding: 10px 12px;
    margin-bottom: 8px; border-left: 3px solid var(--samba-gold); font-size: 12px;
    box-shadow: 0 1px 3px rgba(0,0,0,.04);
}
.kanban-card-name { font-weight: 700; color: var(--samba-ice); margin-bottom: 2px; }
.kanban-card-sub  { color: var(--samba-dim); font-size: 11px; }

/* ── DATAFRAME ───────────────────────────────────────────────────────────── */
[data-testid="stDataFrame"] { background: transparent !important; }

/* ── TABELA PREMIUM (samba-table) ────────────────────────────────────────── */
.samba-table-wrap {
    overflow-x: auto; border-radius: 10px;
    border: 1px solid var(--samba-line); background: var(--samba-bg-card);
    box-shadow: 0 2px 12px rgba(0,0,0,.08);
}
.samba-table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
.samba-table thead tr {
    border-bottom: 2px solid var(--samba-gold);
    background: #1A1C24;
}
.samba-table th {
    padding: 10px 14px; text-align: left;
    font-size: 9px; letter-spacing: 1.5px; font-weight: 700;
    color: #7A8299; text-transform: uppercase;
    font-family: 'Montserrat', sans-serif;
}
.samba-table td {
    padding: 9px 14px; border-bottom: 1px solid var(--samba-line-soft);
    vertical-align: middle; white-space: nowrap; color: var(--samba-ice);
}
.samba-table tbody tr:last-child td { border-bottom: none; }
.samba-table tbody tr:hover td { background: #FEF3E2; }

/* ── PRAÇAS PILLS ────────────────────────────────────────────────────────── */
.praca-pill {
    display: flex; justify-content: space-between; align-items: center;
    padding: 7px 10px; margin-bottom: 4px;
    background: var(--samba-bg-card); border-radius: 8px;
    border: 1px solid var(--samba-line);
}
.praca-name { color: var(--samba-dim); font-size: 11px; flex: 1; }
.praca-rs   { color: var(--samba-ice); font-family: monospace; font-size: 12px; font-weight: 600; padding: 0 8px; }
.praca-usd  { color: var(--samba-gold); font-family: monospace; font-size: 11px; font-weight: 700; }

/* ── EXPANDER ────────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    border: 1px solid var(--samba-line) !important;
    border-radius: 10px !important;
    background: var(--samba-bg-card) !important;
}

/* ── GEOPOLITICAL NEWS FEED ──────────────────────────────────────────────── */
.geo-feed-wrap { margin: 6px 0 18px; }
.geo-feed-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
.geo-feed-title {
    font-size: 9px; letter-spacing: 2px; font-weight: 700;
    color: var(--samba-gold); text-transform: uppercase;
    font-family: 'Montserrat', sans-serif;
}
.geo-feed-meta { font-size: 10px; color: var(--samba-muted); font-style: italic; }
.geo-feed-scroll {
    display: flex; gap: 10px;
    overflow-x: auto; padding-bottom: 8px;
    scrollbar-width: thin;
    scrollbar-color: rgba(250,130,0,.3) transparent;
}
.geo-feed-scroll::-webkit-scrollbar        { height: 4px; }
.geo-feed-scroll::-webkit-scrollbar-track  { background: transparent; }
.geo-feed-scroll::-webkit-scrollbar-thumb  { background: rgba(250,130,0,.3); border-radius: 4px; }
.geo-news-card {
    background: var(--samba-bg-card);
    border-radius: 10px; padding: 12px 14px;
    border-left: 3px solid var(--samba-gold);
    min-width: 270px; max-width: 320px;
    flex-shrink: 0; position: relative;
    border-top: 1px solid var(--samba-line);
    border-right: 1px solid var(--samba-line);
    border-bottom: 1px solid var(--samba-line);
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
    transition: box-shadow 0.2s; cursor: default;
}
.geo-news-card:hover { box-shadow: 0 3px 12px rgba(0,0,0,.08); }
.geo-news-card.critica { border-left-color: #D93025; }
.geo-news-card.alta    { border-left-color: #FA8200; }
.geo-news-card.media   { border-left-color: #C8A000; }
.geo-impact-pill {
    display: inline-block; font-size: 9px; font-weight: 800;
    letter-spacing: 0.8px; text-transform: uppercase;
    padding: 2px 8px; border-radius: 10px; margin-right: 5px; margin-bottom: 7px;
}
.geo-impact-pill.critica { background: #FEECEC; color: #D93025; }
.geo-impact-pill.alta    { background: #FFF4E0; color: #FA8200; }
.geo-impact-pill.media   { background: #FEFBE0; color: #B09000; }
.geo-comm-tag {
    display: inline-block; font-size: 9px; font-weight: 700;
    letter-spacing: 0.5px; text-transform: uppercase;
    padding: 2px 7px; border-radius: 10px; margin-right: 3px; margin-bottom: 7px;
    background: var(--samba-bg-soft); color: var(--samba-dim);
    border: 1px solid var(--samba-line);
}
.geo-news-headline {
    font-size: 12.5px; font-weight: 700; color: var(--samba-ice);
    line-height: 1.45; margin-bottom: 5px;
    display: -webkit-box; -webkit-line-clamp: 2;
    -webkit-box-orient: vertical; overflow: hidden;
}
.geo-news-desc {
    font-size: 10.5px; color: var(--samba-muted); line-height: 1.4;
    margin-bottom: 8px; opacity: .85;
    display: -webkit-box; -webkit-line-clamp: 2;
    -webkit-box-orient: vertical; overflow: hidden;
}
.geo-news-footer {
    display: flex; justify-content: space-between; align-items: center;
    font-size: 10px; color: var(--samba-muted);
}
.geo-news-source { font-weight: 600; color: var(--samba-dim); max-width: 120px;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.geo-news-date   { color: var(--samba-muted); }
.geo-news-link {
    position: absolute; top: 10px; right: 12px;
    font-size: 13px; color: rgba(250,130,0,.4);
    text-decoration: none; line-height: 1; transition: color 0.15s;
}
.geo-news-link:hover { color: var(--samba-gold); }
.geo-feed-empty {
    display: flex; align-items: center; gap: 10px;
    padding: 8px 0 14px; font-size: 11px; color: var(--samba-muted);
}
.geo-feed-empty-dot {
    width: 6px; height: 6px; border-radius: 50%;
    background: var(--samba-line); flex-shrink: 0;
}
</style>
""", unsafe_allow_html=True)

# ========================
# ENGINE
# ========================
_db_url = os.getenv("DATABASE_URL")
if not _db_url:
    try:
        _db_url = st.secrets.get("DATABASE_URL")
    except Exception:
        pass
engine = get_engine(_db_url)  # get_engine(None) usa st.secrets internamente como fallback


# ========================
# QUERIES CACHED
# ========================

@st.cache_data(ttl=60)
def load_kpis():
    try:
        with engine.connect() as conn:
            total_deals = conn.execute(sqlalchemy.text(
                "SELECT COUNT(*) FROM deals WHERE status='ativo'"
            )).scalar() or 0

            total_volume = conn.execute(sqlalchemy.text(
                "SELECT COALESCE(SUM(LEAST(volume,500000)),0) FROM deals "
                "WHERE status='ativo' AND volume_unit='MT' AND volume > 0"
            )).scalar() or 0

            bid_count = conn.execute(sqlalchemy.text(
                "SELECT COUNT(*) FROM deals WHERE status='ativo' AND UPPER(direcao)='BID'"
            )).scalar() or 0

            ask_count = conn.execute(sqlalchemy.text(
                "SELECT COUNT(*) FROM deals WHERE status='ativo' AND UPPER(direcao)='ASK'"
            )).scalar() or 0

            cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=24)).isoformat()
            deals_hoje = conn.execute(sqlalchemy.text(
                f"SELECT COUNT(*) FROM deals WHERE created_at >= '{cutoff}'"
            )).scalar() or 0

        return {
            "total_deals":    int(total_deals),
            "total_volume_mt": float(total_volume),
            "bid_count":      int(bid_count),
            "ask_count":      int(ask_count),
            "deals_hoje":     int(deals_hoje),
        }
    except Exception:
        return {"total_deals": 0, "total_volume_mt": 0.0, "bid_count": 0, "ask_count": 0, "deals_hoje": 0}


@st.cache_data(ttl=60)
def load_pracas(produto: str):
    if market_data is None:
        import pandas as _pd
        return _pd.DataFrame()
    df = market_data.get_pracas_fisicas(produto)
    return df


# Filtro base de higiene: remove arquivos-lixo e commodity nula/indefinida.
_NOISE_FILTER = (
    " AND LOWER(COALESCE(name,'')) NOT LIKE '%.xlsx%'"
    " AND LOWER(COALESCE(name,'')) NOT LIKE '%.html%'"
    " AND LOWER(COALESCE(name,'')) NOT LIKE '%.xls%'"
    " AND LOWER(COALESCE(commodity,'')) NOT IN ('indefinida','indefinido','')"
)

# Whitelist de termos validos de commodity. Um deal so aparece nas views
# publicas se o campo commodity contiver pelo menos um desses termos.
# Isso elimina extracao errada (nomes de pessoa, grupos, etc.).
_COMMODITY_WHITELIST_SQL = """
  AND (
    LOWER(COALESCE(commodity,'')) LIKE '%soja%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%milho%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%acucar%'  OR
    LOWER(COALESCE(commodity,'')) LIKE '%açúcar%'  OR
    LOWER(COALESCE(commodity,'')) LIKE '%ic45%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%arroz%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%frango%'  OR
    LOWER(COALESCE(commodity,'')) LIKE '%algod%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%etanol%'  OR
    LOWER(COALESCE(commodity,'')) LIKE '%cacau%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%caf%'     OR
    LOWER(COALESCE(commodity,'')) LIKE '%leo%'     OR
    LOWER(COALESCE(commodity,'')) LIKE '%feij%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%prata%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%ouro%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%boi%'     OR
    LOWER(COALESCE(commodity,'')) LIKE '%carne%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%farelo%'  OR
    LOWER(COALESCE(commodity,'')) LIKE '%diesel%'  OR
    LOWER(COALESCE(commodity,'')) LIKE '%coco%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%trigo%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%chicken%' OR
    LOWER(COALESCE(commodity,'')) LIKE '%pork%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%porco%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%mineral%' OR
    LOWER(COALESCE(commodity,'')) LIKE '%ureia%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%npk%'     OR
    LOWER(COALESCE(commodity,'')) LIKE '%girassol%'OR
    LOWER(COALESCE(commodity,'')) LIKE '%palma%'   OR
    LOWER(COALESCE(commodity,'')) LIKE '%acem%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%pesc%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%atum%'    OR
    LOWER(COALESCE(commodity,'')) LIKE '%lagost%'  OR
    LOWER(COALESCE(commodity,'')) LIKE '%tilapia%'
  )
"""


@st.cache_data(ttl=300)
def load_deals_recentes():
    try:
        return pd.read_sql(f"""
            WITH ranked AS (
              SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY LOWER(TRIM(COALESCE(name,'')))
                  ORDER BY created_at DESC
                ) AS rn
              FROM deals
              WHERE status='ativo'
                AND UPPER(COALESCE(direcao,'')) IN ('BID','ASK')
                {_NOISE_FILTER}
                {_COMMODITY_WHITELIST_SQL}
            )
            SELECT
                name          AS "Deal",
                commodity     AS "Commodity",
                UPPER(direcao) AS "Dir",
                CASE WHEN price IS NOT NULL AND price > 0
                     THEN CONCAT(ROUND(price::numeric,2)::text, ' ', COALESCE(currency,'USD'))
                     ELSE 'A Definir' END AS "Preco",
                COALESCE(stage, 'Lead Capturado') AS "Stage",
                COALESCE(source_group, COALESCE(source_sender, '-')) AS "Grupo",
                LEFT(created_at::text, 10) AS "Data"
            FROM ranked
            WHERE rn = 1
            ORDER BY created_at DESC
            LIMIT 15
        """, engine)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_deals_ia_enriched():
    try:
        return pd.read_sql(f"""
            SELECT
                name       AS "Deal",
                commodity  AS "Commodity",
                risk_score AS "Score",
                CASE
                    WHEN risk_score <= 25 THEN 'BAIXO'
                    WHEN risk_score <= 50 THEN 'MEDIO'
                    WHEN risk_score <= 75 THEN 'ALTO'
                    ELSE 'CRITICO'
                END AS "Nivel"
            FROM deals
            WHERE risk_score IS NOT NULL AND risk_score != 50 AND status='ativo'
              {_NOISE_FILTER} {_COMMODITY_WHITELIST_SQL}
            ORDER BY risk_score DESC
            LIMIT 20
        """, engine)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_pipeline_por_stage():
    try:
        return pd.read_sql(f"""
            SELECT
                id,
                name       AS "Deal",
                commodity  AS "Commodity",
                UPPER(COALESCE(direcao,'?')) AS "Dir",
                CASE WHEN price IS NOT NULL
                     THEN CONCAT(ROUND(price::numeric,2)::text, ' ', COALESCE(currency,'USD'))
                     ELSE 'Preco a Definir' END AS "Preco",
                COALESCE(source_group, '-') AS "Cliente",
                COALESCE(stage, 'Lead Capturado') AS "Stage",
                risk_score AS "Risco"
            FROM deals
            WHERE status='ativo' {_NOISE_FILTER} {_COMMODITY_WHITELIST_SQL}
            ORDER BY created_at DESC
        """, engine)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_arbitragem():
    try:
        return pd.read_sql(f"""
            WITH ranked AS (
              SELECT *,
                ROW_NUMBER() OVER (
                  PARTITION BY LOWER(TRIM(COALESCE(name,'')))
                  ORDER BY created_at DESC
                ) AS rn
              FROM deals
              WHERE status='ativo'
                AND UPPER(COALESCE(direcao,'')) IN ('BID','ASK')
                AND price IS NOT NULL
                AND price > 50
                AND price < 6000
                {_NOISE_FILTER}
                {_COMMODITY_WHITELIST_SQL}
            )
            SELECT
                commodity                   AS "Commodity",
                UPPER(direcao)              AS "Direcao",
                price                       AS "Preco",
                COALESCE(currency,'USD')    AS "Moeda",
                volume                      AS "Volume",
                COALESCE(volume_unit,'MT')  AS "Unid",
                COALESCE(incoterm,'-')      AS "Incoterm",
                COALESCE(source_group, source_sender, '-') AS "Cliente",
                COALESCE(destination, origin, '-') AS "Porto"
            FROM ranked
            WHERE rn = 1
            ORDER BY commodity, direcao
        """, engine)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=120)
def load_book():
    """Order book: melhor ASK e BID por commodity — apenas BID/ASK com preco valido."""
    try:
        return pd.read_sql(f"""
            SELECT
                commodity                                        AS "Commodity",
                MIN(CASE WHEN UPPER(direcao)='ASK' THEN price END) AS "Best ASK",
                MAX(CASE WHEN UPPER(direcao)='BID' THEN price END) AS "Best BID",
                COUNT(DISTINCT LOWER(TRIM(COALESCE(name,''))))   AS "Deals"
            FROM deals
            WHERE status='ativo'
              AND UPPER(COALESCE(direcao,'')) IN ('BID','ASK')
              AND price > 50 AND price < 6000
              {_NOISE_FILTER}
              {_COMMODITY_WHITELIST_SQL}
            GROUP BY commodity
            HAVING "Best ASK" IS NOT NULL OR "Best BID" IS NOT NULL
            ORDER BY "Deals" DESC
        """, engine)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_market():
    """Market overview cacheado (SOY, CORN, SUGAR, USD/BRL — para KPI cards)."""
    if market_data is None:
        return {}
    try:
        result = market_data.get_market_overview()
        # Se DB retornou zeros, busca ao vivo via batch (1 chamada, não 4)
        if result.get("SOY_CBOT (USD/MT)", {}).get("valor", 0) == 0:
            raise ValueError("zero values from DB")
        return result
    except Exception:
        pass
    try:
        from services.market_data import ExternalDataService as _EDS
        closes = _EDS._fetch_batch()

        def _last(sym):
            vals = _EDS._close(closes, sym)
            return float(vals[-1]) if vals else 0.0

        _soy  = _last("ZS=F")
        _corn = _last("ZC=F")
        _sug  = _last("SB=F")
        _fx   = _last("USDBRL=X")
        return {
            "SOY_CBOT (USD/MT)":  {"valor": round((_soy  / 100) * 36.7437, 2), "variacao": 0.0},
            "CORN_CBOT (USD/MT)": {"valor": round((_corn / 100) * 39.3680, 2), "variacao": 0.0},
            "SUGAR_ICE (USD/MT)": {"valor": round(_sug * 22.0462, 2),          "variacao": 0.0},
            "USD/BRL":            {"valor": round(_fx, 4),                      "variacao": 0.0},
        }
    except Exception:
        return {}


@st.cache_data(ttl=300)
def load_extended_market() -> dict:
    """
    Overview estendido para o Ticker: base + FARELO, CACAU, CAFE, ALGODAO.
    Cache 5 min — batch download único (1 chamada HTTP, não 8 sequenciais).
    """
    if market_data is None:
        return {}
    try:
        if hasattr(market_data, "get_extended_overview"):
            return market_data.get_extended_overview()
    except Exception:
        pass
    # Fallback direto via batch — evita 4 chamadas sequenciais
    try:
        from services.market_data import ExternalDataService as _EDS
        base   = market_data.get_market_overview()
        closes = _EDS._fetch_batch()
        _EXTRA = {
            "ZM=F": ("FARELO SOJA (USD/MT)", lambda p: p * 1.10231, 320.0),
            "CC=F": ("CACAU ICE (USD/MT)",   lambda p: p,            8000.0),
            "KC=F": ("CAFE ICE (USD/MT)",    lambda p: p * 22.0462,  5500.0),
            "CT=F": ("ALGODAO ICE (USD/MT)", lambda p: p * 22.0462,  1700.0),
        }
        ordered = {k: v for k, v in base.items() if k != "USD/BRL"}
        for sym, (key, conv, fb) in _EXTRA.items():
            try:
                vals = _EDS._close(closes, sym)
                if vals:
                    cur  = conv(float(vals[-1]))
                    prev = conv(float(vals[-2])) if len(vals) >= 2 else cur
                    var  = ((cur - prev) / prev * 100) if prev else 0.0
                    ordered[key] = {"valor": round(cur, 2), "variacao": round(var, 4)}
                else:
                    ordered[key] = {"valor": fb, "variacao": 0.0}
            except Exception:
                ordered[key] = {"valor": fb, "variacao": 0.0}
        if "USD/BRL" in base:
            ordered["USD/BRL"] = base["USD/BRL"]
        return ordered
    except Exception:
        return market_data.get_market_overview() if market_data else {}


def _load_geo_feed(force_api: bool = False) -> dict:
    """
    Carrega alertas geopolíticos.

    Prioridade:
      1. Arquivo data/geo_alerts_cache.json escrito pelo Celery Beat.
      2. Se arquivo ausente OU force_api=True: chama NewsData.io diretamente
         (usa chaves do .env; silencioso se não configuradas).

    Sem @st.cache_data — leitura local é instantânea; API call fica no spinner.
    """
    _cache_file = ROOT / "data" / "geo_alerts_cache.json"

    # ── Lê cache (Celery Beat OU última chamada direta) ────────────
    # TTL diferenciado:
    #   • Com alertas  → 6h  (quota preciosa, resultado estável)
    #   • Sem alertas  → 30min (força nova tentativa mais cedo)
    if not force_api and _cache_file.exists():
        try:
            data = json.loads(_cache_file.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "alerts" in data:
                import datetime as _dtt
                ts_raw = data.get("ts", "")
                if ts_raw:
                    try:
                        cache_age = (_dtt.datetime.utcnow()
                                     - _dtt.datetime.fromisoformat(ts_raw[:19]))
                        has_alerts = bool(data.get("alerts"))
                        ttl_sec    = 6 * 3600 if has_alerts else 30 * 60
                        if cache_age.total_seconds() < ttl_sec:
                            return data  # cache válido
                    except Exception:
                        pass
                else:
                    return data  # sem timestamp → aceita mesmo assim
        except Exception:
            pass

    # ── Fallback: chamada direta à API (primeira carga ou botão refresh) ──
    try:
        import os as _os
        _has_key = bool(
            _os.getenv("NEWSDATA_KEY_1") or
            _os.getenv("NEWSDATA_KEY_2") or
            _os.getenv("NEWS_API_KEY")
        )
        if not _has_key:
            return {"ts": "", "alerts": [], "source": "no_key"}

        from services.news_intelligence import run_geopolitical_scan, _available_keys
        import datetime as _dt
        # Verifica quota antes de chamar (todas as chaves esgotadas = 429)
        if not _available_keys():
            return {"ts": _dt.datetime.utcnow().isoformat(), "alerts": [], "source": "quota_exceeded"}
        alerts = run_geopolitical_scan(hours_back=12)
        # Se voltou vazio mas chaves sumiram durante a chamada → quota esgotada
        _src = "quota_exceeded" if (not alerts and not _available_keys()) else "api"
        result = {"ts": _dt.datetime.utcnow().isoformat(), "alerts": alerts, "source": _src}

        # Persiste para próximas leituras.
        # Resultado vazio (api sem alertas) expira em 30min para retentar logo.
        # Resultado com alertas expira em 6h (não reabre quota desnecessariamente).
        try:
            _cache_file.parent.mkdir(exist_ok=True)
            _cache_file.write_text(
                json.dumps(result, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        except Exception:
            pass

        return result
    except Exception:
        return {"ts": "", "alerts": [], "source": "error"}


# ========================
# INVENTARIO DOCUMENTAL (snapshot para badges no Kanban)
# ========================
DOC_TYPES_PRIORITY = ["LOI", "ICPO", "SCO", "FCO", "SPA", "SBLC", "RWA", "Ficha de Cadastro"]


@st.cache_data(ttl=300)
def load_doc_inventory() -> dict:
    """Le snapshot gerado por scripts/scan_drive_inventory.py.

    Retorna indice: {cliente_lower: set(doc_types_presentes)}
    """
    import json as _json
    snap = ROOT / "data" / "doc_inventory_snapshot.json"
    if not snap.exists():
        return {"by_client": {}, "stats": {}, "generated_at": None, "sheet_id": None}
    try:
        data = _json.loads(snap.read_text(encoding="utf-8"))
    except Exception:
        return {"by_client": {}, "stats": {}, "generated_at": None, "sheet_id": None}

    # Cada row = [cliente_path, nome, tipo, data, link]
    by_client: dict[str, set] = {}
    for row in data.get("rows", []):
        if len(row) < 3:
            continue
        cliente_path, _name, tipo = row[0], row[1], row[2]
        if tipo in ("Ruido", "Outro"):
            continue
        # Normaliza pelo ultimo segmento do path (ex.: "COMERCIAL / Cliente ABC / Soja")
        parts = [p.strip() for p in cliente_path.split("/") if p.strip() and p.strip() != "COMERCIAL"]
        if not parts:
            continue
        root_client = parts[0].lower()
        by_client.setdefault(root_client, set()).add(tipo)

    return {
        "by_client": by_client,
        "stats": data.get("stats", {}),
        "generated_at": data.get("generated_at"),
        "sheet_id": data.get("sheet_id"),
    }


def render_doc_badges(deal_client: str, inventory: dict) -> str:
    """Retorna HTML com badges dos docs encontrados para esse cliente.

    Combina match exato + match parcial por substring.
    """
    by_client = inventory.get("by_client", {})
    if not deal_client or not by_client:
        return '<span class="doc-badge-muted">sem docs</span>'

    needle = deal_client.lower().strip()
    docs: set = set()
    for client_key, doc_set in by_client.items():
        # match por substring em qualquer direcao (nome do grupo WhatsApp x nome da pasta)
        if needle in client_key or client_key in needle:
            docs |= doc_set

    if not docs:
        return '<span class="doc-badge-muted">sem docs</span>'

    # Renderiza na ordem de prioridade do funil documental
    ordered = [t for t in DOC_TYPES_PRIORITY if t in docs]
    html = "".join(f'<span class="doc-badge">📄 {t}</span>' for t in ordered)
    return html


@st.cache_data(ttl=120)
def load_compliance_seals() -> dict[int, dict]:
    """
    Retorna mapa {deal_id: {status, score, doc_type, audited_at}}
    com a ULTIMA auditoria de cada deal (para selos no Kanban).
    """
    try:
        from models.database import DocumentCompliance, get_session
        sess = get_session()
        records = (
            sess.query(DocumentCompliance)
            .order_by(DocumentCompliance.audited_at.desc())
            .all()
        )
        sess.close()
        seals: dict[int, dict] = {}
        for r in records:
            if r.deal_id and r.deal_id not in seals:
                seals[r.deal_id] = {
                    "status": r.status or "—",
                    "score": r.score or 0,
                    "doc_type": r.document_type or "—",
                    "audited_at": r.audited_at.strftime("%d/%m") if r.audited_at else "—",
                }
        return seals
    except Exception:
        return {}


def render_compliance_seal(deal_id: int, seals: dict) -> str:
    """Retorna HTML do selo de compliance para o card do Kanban."""
    if not deal_id or deal_id not in seals:
        return ""
    info = seals[deal_id]
    status = info["status"]
    colors = {"VERDE": "#329632", "AMARELO": "#fa8200", "VERMELHO": "#fa3232"}
    icons = {"VERDE": "✅", "AMARELO": "⚠️", "VERMELHO": "❌"}
    color = colors.get(status, "#9a9aa0")
    icon = icons.get(status, "•")
    return (
        f'<span style="display:inline-block;background:{color}22;border:1px solid {color}44;'
        f'border-radius:4px;padding:1px 6px;font-size:10px;color:{color};margin-right:4px">'
        f'{icon} {status} {info["score"]}/100 · {info["doc_type"]}</span>'
    )


# ──────────────────────────────────────────────────────────────────
# PLANILHA — leitura das abas de negócio (fonte primária de verdade)
# ──────────────────────────────────────────────────────────────────
_SHEET_ID = "1ToNZxYYi0dPQkQ0bRE8W3DWXJxkzEVS4vLQyrj2VP9U"

# Mapeamento visual de status da planilha → (label, cor)
_STATUS_MAP = {
    "pendente comprador":    ("AGUARDANDO COMPRADOR", "#fa8200"),
    "pendente vendedor":     ("AGUARDANDO FORNECEDOR", "#326496"),
    "pendente samba":        ("AGUARDANDO SAMBA", "#64c8fa"),
    "em deal":               ("EM NEGOCIAÇÃO", "#329632"),
    "em negociação":         ("EM NEGOCIAÇÃO", "#329632"),
    "procurar comprador":    ("PROSPECÇÃO ATIVA", "#fa8200"),
    "aguardando reunião":    ("REUNIÃO AGENDADA", "#64c8fa"),
    "parada":                ("EM ESPERA", "#666666"),
    "lead capturado":        ("LEAD WHATSAPP", "#64c8fa"),
    "abandonado pela samba": ("ARQUIVADO", "#444444"),
    "perdido":               ("PERDIDO", "#fa3232"),
    "concluído":             ("CONCLUÍDO", "#329632"),
}

# Mapa emoji de produto (expansível)
_PRODUTO_EMOJI = {
    "soja": "🌿", "milho": "🌽", "acucar": "🍬", "açúcar": "🍬", "ic45": "🍬",
    "frango": "🍗", "chicken paw": "🍗", "arroz": "🍚", "algodao": "☁️",
    "algodão": "☁️", "etanol": "⛽", "diesel": "⛽", "cacau": "🍫",
    "cafe": "☕", "café": "☕", "ouro": "🥇", "prata": "🥈",
    "farelo": "🌾", "farelo soja": "🌾", "farelo peixe": "🐟",
    "milho non gmo": "🌽", "soja non gmo": "🌿", "oleo": "🛢️",
    "óleo": "🛢️", "ureia": "🧪", "npk": "🧪",
    "granito": "🪨", "cpr": "📋", "cpf": "📋",
}


def _prod_emoji(produto: str) -> str:
    p = (produto or "").lower().strip()
    for k, e in _PRODUTO_EMOJI.items():
        if k in p:
            return e
    return "📦"


def _status_badge_html(status_raw: str, small: bool = False) -> str:
    key = (status_raw or "").lower().strip()
    label, cor = _STATUS_MAP.get(key, (status_raw.upper() or "—", "#666"))
    size = "9px" if small else "10px"
    return (
        f'<span style="background:{cor}22;border:1px solid {cor}55;color:{cor};'
        f'padding:2px 7px;border-radius:10px;font-size:{size};font-weight:700;'
        f'white-space:nowrap">{label}</span>'
    )


def _norm_grupo(g: str) -> str:
    """Normaliza nome de grupo/parceiro para cross-reference."""
    g = (g or "").lower().strip()
    for prefix in ("samba x ", "samba ", "commercial samba x "):
        if g.startswith(prefix):
            g = g[len(prefix):]
    return g


@st.cache_data(ttl=120)
def _load_planilha_pipeline() -> list[dict]:
    """
    Lê 'todos andamento' + 'Andamento Vietnã' do Google Sheets.
    Retorna lista de dicts normalizados, prontos para exibição.
    """
    try:
        from services.google_drive import drive_manager
        from googleapiclient.discovery import build
        svc = build("sheets", "v4", credentials=drive_manager.creds)

        def _read(tab_name: str) -> list[list]:
            r = svc.spreadsheets().values().get(
                spreadsheetId=_SHEET_ID, range=f"'{tab_name}'"
            ).execute()
            return r.get("values", [])

        def _parse(raw: list[list], fonte: str) -> list[dict]:
            if len(raw) < 5:
                return []
            results = []
            for row in raw[4:]:
                def g(i): return row[i].strip() if i < len(row) else ""
                job = g(0)
                if not job and not g(6):
                    continue   # linha vazia
                results.append({
                    "job":         job or "—",
                    "data":        g(1),
                    "tipo":        g(2),             # pedido / oferta
                    "grupo":       g(3),
                    "solicitante": g(4),
                    "status_raw":  g(5),
                    "produto":     g(6),
                    "comprador":   g(7),
                    "fornecedor":  g(8),
                    "vis_rapida":  g(9),
                    "docs":        g(10),
                    "especificacao": g(11),
                    "situacao":    g(12),
                    "acao":        g(13),
                    "responsavel": g(14),            # RESPONSÁVEL (Vietnã) / STATUS_AUTO (todos)
                    "fonte":       fonte,
                    "grupo_norm":  _norm_grupo(g(3)),
                })
            return results

        deals = _parse(_read("todos andamento"), "🇧🇷 Andamento")
        deals += _parse(_read("Andamento Vietnã"), "🇻🇳 Vietnã")
        return deals
    except Exception as _e:
        return []


@st.cache_data(ttl=300)
def _load_declinados() -> list[dict]:
    """
    Le aba 'Declinados' do Google Sheets (118 deals arquivados).
    Mesma estrutura de colunas de 'todos andamento'.
    Usado para PROC V de follow-ups e visualizacao historica no Pipeline.
    """
    try:
        from services.google_drive import drive_manager
        from googleapiclient.discovery import build
        svc = build("sheets", "v4", credentials=drive_manager.creds)
        r = svc.spreadsheets().values().get(
            spreadsheetId=_SHEET_ID, range="'Declinados'"
        ).execute()
        raw = r.get("values", [])
        if len(raw) < 5:
            return []
        results = []
        for row in raw[4:]:
            def g(i, _row=row): return _row[i].strip() if i < len(_row) else ""
            job = g(0)
            if not job and not g(6):
                continue   # linha vazia
            results.append({
                "job":           job or "—",
                "data":          g(1),
                "tipo":          g(2),
                "grupo":         g(3),
                "solicitante":   g(4),
                "status_raw":    g(5) or "declinado",
                "produto":       g(6),
                "comprador":     g(7),
                "fornecedor":    g(8),
                "vis_rapida":    g(9),
                "docs":          g(10),
                "especificacao": g(11),
                "situacao":      g(12),
                "acao":          g(13),
                "responsavel":   g(14),
                "fonte":         "🗂️ Declinados",
                "grupo_norm":    _norm_grupo(g(3)),
            })
        return results
    except Exception:
        return []


@st.cache_data(ttl=120)
def _sheet_enrich_index() -> dict[str, dict]:
    """
    Cria indice {grupo_norm: deal_dict} a partir de TODAS as abas de andamento
    (todos andamento + Andamento Vietna + Declinados).
    Usado para enriquecer follow-ups com contexto da planilha (PROC V).
    Declinados tem prioridade menor: so preenche chave se ainda nao existir.
    """
    idx: dict[str, dict] = {}

    # Abas ativas — prioridade maxima
    for d in _load_planilha_pipeline():
        key = d["grupo_norm"]
        if key and key not in idx:
            idx[key] = d
        if d["job"] and d["job"] != "—":
            idx[d["job"].lower()] = d

    # Declinados — enriquece sem sobrescrever ativos
    for d in _load_declinados():
        key = d["grupo_norm"]
        if key and key not in idx:
            idx[key] = d
        job_key = (d["job"] or "").lower()
        if job_key and job_key != "—" and job_key not in idx:
            idx[job_key] = d

    return idx


def _get_cambio(ov: dict) -> float:
    try:
        return float(ov.get("USD/BRL", {}).get("valor", 5.19)) or 5.19
    except Exception:
        return 5.19


def _agent_last_run(log_glob: str):
    """Retorna (datetime | None) do ultimo evento em data/logs/<glob>.jsonl"""
    import glob as _glob
    import json as _json
    files = sorted(_glob.glob(str(ROOT / "data" / "logs" / log_glob)))
    if not files:
        return None
    try:
        with open(files[-1], "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
        if not lines:
            return None
        last = _json.loads(lines[-1])
        ts = last.get("timestamp") or last.get("ts") or last.get("time")
        if ts:
            return datetime.datetime.fromisoformat(str(ts)[:19])
    except Exception:
        pass
    return None


# ========================
# HEADER — dark full-bleed HTML strip (Brand Manual pág. 3)
# ========================
import base64 as _b64

logo_path = ROOT / "assets" / "logo.png"
_logo_b64 = ""
if logo_path.exists():
    with open(logo_path, "rb") as _lf:
        _logo_b64 = _b64.b64encode(_lf.read()).decode()
_logo_tag = (
    f'<img src="data:image/png;base64,{_logo_b64}" '
    f'style="height:46px;width:auto;flex-shrink:0;display:block">'
    if _logo_b64 else ""
)

_agora = datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')

st.markdown(f"""
<style>
/* ── DARK HEADER STRIP: hidden trigger buttons ── */
.st-key-cd_docs_hidden,
.st-key-cd_portal_hidden {{
    position: absolute !important;
    left: -9999px !important;
    height: 0 !important;
    overflow: hidden !important;
    visibility: hidden !important;
}}
/* ── Header alinhado com área de conteúdo dinâmico ── */
.samba-hdr {{
    background: #000000;
    border-bottom: 3px solid #FA8200;
    padding: 16px 24px 14px;
    margin-top: 0;
    border-radius: 10px;
    display: flex;
    align-items: center;
    gap: 20px;
    min-height: 82px;
    box-sizing: border-box;
    box-shadow: 0 4px 18px rgba(0,0,0,.35);
}}
.samba-hdr-btn {{
    width:34px; height:34px; border-radius:7px;
    background:rgba(255,255,255,0.06);
    border:1px solid rgba(255,255,255,0.12);
    display:inline-flex; align-items:center; justify-content:center;
    cursor:pointer; text-decoration:none; color:#BFBFBF;
    transition:all .15s; flex-shrink:0;
}}
.samba-hdr-btn:hover {{
    border-color:#FA8200; color:#FA8200;
    background:rgba(250,130,0,0.14);
}}
.samba-hdr-btn svg {{ width:15px; height:15px; }}
</style>
<div class="samba-hdr">
  {_logo_tag}
  <div style="flex:1;min-width:0">
    <div style="font-family:Montserrat,sans-serif;font-size:13px;letter-spacing:3.5px;color:#FA8200;font-weight:800;white-space:nowrap">GLOBAL COMMODITIES CONTROL DESK</div>
    <div style="font-family:Montserrat,sans-serif;font-size:12px;font-weight:500;color:#BFBFBF;letter-spacing:1px;margin-top:4px">Export Intelligence Platform</div>
  </div>
  <div style="display:flex;gap:8px;align-items:center;flex-shrink:0">
    <button class="samba-hdr-btn" title="← Portal"
      onclick="(function(){{var d=document;var b=d.querySelector('.st-key-cd_portal_hidden button');if(!b){{try{{b=window.parent.document.querySelector('.st-key-cd_portal_hidden button');}}catch(e){{}}}}if(b)b.click();}})()">
      <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">
        <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
        <polyline points="9 22 9 12 15 12 15 22"/>
      </svg>
    </button>
    <button class="samba-hdr-btn" title="Gerador de Documentos"
      onclick="(function(){{var b=window.parent.document.querySelector('.st-key-cd_docs_hidden button');if(b)b.click();}})()">
      <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
        <polyline points="14 2 14 8 20 8"/>
        <line x1="16" y1="13" x2="8" y2="13"/>
        <line x1="16" y1="17" x2="8" y2="17"/>
      </svg>
    </button>
    <a href="https://drive.google.com/drive/folders/0AOllQoxhuNj4Uk9PVA" target="_blank"
       class="samba-hdr-btn" title="Google Drive Corporativo">
      <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">
        <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
      </svg>
    </a>
    <span class="samba-hdr-btn" title="Em breve" style="opacity:.28;cursor:default">
      <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">
        <circle cx="12" cy="12" r="3"/>
        <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>
      </svg>
    </span>
    <span class="samba-hdr-btn" title="Em breve" style="opacity:.28;cursor:default">
      <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">
        <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
        <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
      </svg>
    </span>
  </div>
  <div style="text-align:right;font-family:Montserrat,sans-serif;flex-shrink:0;margin-left:8px">
    <div style="font-size:9px;letter-spacing:1.8px;color:#7F7F7F;font-weight:700;text-transform:uppercase">UPDATED</div>
    <div style="font-weight:800;color:#FFFFFF;font-size:15px;margin-top:3px;letter-spacing:.3px">{_agora}</div>
  </div>
</div>
""", unsafe_allow_html=True)

# Hidden Streamlit button — triggered by the HTML docs icon via JS onclick
if st.button("__docs__", key="cd_docs_hidden", help="Gerador de Documentos"):
    st.session_state.prev_view = "operacoes"
    st.session_state.current_view = "documentos"
    st.rerun()

# Hidden portal button — triggered by the HTML home icon in the header
# (sidebar colapsado não tem DOM ativo; este botão fica no conteúdo principal)
if st.button("__portal__", key="cd_portal_hidden", help="← Portal"):
    st.session_state.current_view = "portal"
    st.rerun()

# ========================
# TICKER
# ========================
# Mapa de rótulos curtos para exibição no ticker (sem unidade repetitiva)
_TICKER_LABELS: dict[str, str] = {
    "SOY_CBOT (USD/MT)":     "SOJA CBOT",
    "CORN_CBOT (USD/MT)":    "MILHO CBOT",
    "SUGAR_ICE (USD/MT)":    "AÇÚCAR ICE",
    "FARELO SOJA (USD/MT)":  "FARELO SOJA",
    "CACAU ICE (USD/MT)":    "CACAU ICE",
    "CAFE ICE (USD/MT)":     "CAFÉ ICE",
    "ALGODAO ICE (USD/MT)":  "ALGODÃO ICE",
    "USD/BRL":               "USD/BRL",
}

# Separador visual entre itens
_TICKER_SEP = '<span style="color:#2a2a34;padding:0 8px">|</span>'

try:
    _ext_ov = load_extended_market()
    _ticker_itens = ""
    for _ in range(4):   # 4 repetições — ciclo longo sem reinício visível
        for _key, _info in _ext_ov.items():
            _val  = _info.get("valor", 0)
            _var  = _info.get("variacao", 0)
            _cor  = "t-up" if _var > 0 else "t-down" if _var < 0 else "t-ativo"
            _sign = "+" if _var > 0 else ""
            _lbl  = _TICKER_LABELS.get(_key, _key)

            # Câmbio sem unidade USD/MT
            _val_str = (
                f"{_val:.4f}"
                if _key == "USD/BRL"
                else f"USD {_val:,.0f}/MT"
            )

            _ticker_itens += (
                f'<div class="ticker-item">'
                f'<span style="color:var(--samba-dim);font-size:10px;'
                f'letter-spacing:1px;margin-right:5px">{_lbl}</span>'
                f'<span style="color:var(--samba-ice);font-weight:700;'
                f'font-family:monospace">{_val_str}</span>'
                f'&nbsp;<span class="{_cor}" style="font-size:11px">'
                f'{_sign}{_var:.2f}%</span>'
                f'</div>'
                f'{_TICKER_SEP}'
            )
    st.markdown(
        f'<div class="ticker-wrap"><div class="ticker">{_ticker_itens}</div></div>',
        unsafe_allow_html=True,
    )
except Exception:
    st.warning("Ticker indisponivel")

# ========================
# KPI CARDS
# ========================
st.markdown('<div style="height:22px"></div>', unsafe_allow_html=True)
kpis = load_kpis()
_ov  = load_market()

_soy_val  = _ov.get("SOY_CBOT (USD/MT)", {}).get("valor", 0.0)
_soy_var  = _ov.get("SOY_CBOT (USD/MT)", {}).get("variacao", 0.0)
_sug_val  = _ov.get("SUGAR_ICE (USD/MT)", {}).get("valor", 0.0)
_sug_var  = _ov.get("SUGAR_ICE (USD/MT)", {}).get("variacao", 0.0)
_fx_val   = _ov.get("USD/BRL", {}).get("valor", 0.0)
_fx_var   = _ov.get("USD/BRL", {}).get("variacao", 0.0)

def _var_html(v: float) -> str:
    """Variação % com cores legíveis sobre fundo #FA8200 dos KPI cards."""
    if v > 0:
        return f'<span style="color:rgba(255,255,255,.95);font-weight:700">+{v:.2f}%</span>'
    elif v < 0:
        return f'<span style="color:#FFD0D0;font-weight:700">{v:.2f}%</span>'
    return f'<span style="color:rgba(255,255,255,.55)">0.00%</span>'

c1, c2, c3, c4, c5 = st.columns(5)

with c1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">DEALS ATIVOS</div>
        <div class="kpi-value">{kpis['total_deals']:,}</div>
        <div class="kpi-sub">no pipeline</div>
    </div>
    """, unsafe_allow_html=True)

with c2:
    bid = kpis["bid_count"]
    ask = kpis["ask_count"]
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">BID / ASK</div>
        <div class="kpi-value">
            <span style="color:#FFFFFF">{bid}</span>
            <span style="font-size:18px;color:rgba(255,255,255,0.45)"> / </span>
            <span style="color:rgba(255,255,255,0.80)">{ask}</span>
        </div>
        <div class="kpi-sub">compras BID &middot; vendas ASK</div>
    </div>
    """, unsafe_allow_html=True)

with c3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">SOJA CBOT</div>
        <div class="kpi-value">{_soy_val:.2f}</div>
        <div class="kpi-sub">USD/MT &middot; {_var_html(_soy_var)}</div>
    </div>
    """, unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">ACUCAR ICE</div>
        <div class="kpi-value">{_sug_val:.2f}</div>
        <div class="kpi-sub">USD/MT &middot; {_var_html(_sug_var)}</div>
    </div>
    """, unsafe_allow_html=True)

with c5:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">DOLAR PTAX</div>
        <div class="kpi-value">{_fx_val:.3f}</div>
        <div class="kpi-sub">USD/BRL &middot; {_var_html(_fx_var)}</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='margin-bottom:4px'></div>", unsafe_allow_html=True)

# ========================
# BOTÃO ATUALIZAR MERCADO
# ========================
_mkt_col, _mkt_spacer = st.columns([3, 7])
with _mkt_col:
    _mkt_refresh = st.button("⟳ Atualizar Mercado", key="mkt_update_btn", help="Busca dados atualizados de Bolsas (CBOT/ICE), câmbio e praças físicas")
if _mkt_refresh:
    try:
        # Limpa cache imediatamente — próximo render busca dados frescos
        load_market.clear()
        load_extended_market.clear()
        # Dispara update em thread separada — não bloqueia o Streamlit
        import threading as _threading
        from services.market_data import update_all_market_data as _upd_mkt, ExternalDataService as _EDS
        def _bg_update():
            try:
                _EDS._yf_batch_ts = 0.0   # invalida cache batch
                _upd_mkt()
            except Exception:
                pass
        _threading.Thread(target=_bg_update, daemon=True).start()
        st.toast("Atualizando mercado em segundo plano — dados frescos em ~15s", icon="📡")
        st.rerun()
    except Exception as _mkt_err:
        st.error(f"Erro ao iniciar atualização: {_mkt_err}")

# ========================
# GEOPOLITICAL NEWS FEED
# ========================
_IMPACT_CLS   = {"critica": "critica", "alta": "alta", "media": "media"}
_IMPACT_ICON  = {"critica": "🔴 CRÍTICO", "alta": "🟠 ALTO", "media": "🟡 MÉDIO"}
_COMM_DISPLAY = {
    "soja": "SOJA", "acucar": "ACUCAR", "milho": "MILHO",
    "frango": "FRANGO", "oleo": "OLEO", "farelo": "FARELO",
    "global": "GLOBAL",
}

# Cabeçalho + botão de refresh (em linha)
_gcol_title, _gcol_btn = st.columns([8, 1])
with _gcol_title:
    st.markdown(
        '<div class="geo-feed-title" style="padding:6px 0 4px">🌍 SENTINEL GEOPOLÍTICO — IMPACTO NO PIPELINE</div>',
        unsafe_allow_html=True,
    )
with _gcol_btn:
    _geo_refresh = st.button("↻ Refresh", key="geo_refresh", help="Buscar notícias agora")

# Carrega dados (ou forçar via API se botão pressionado)
with st.spinner("Buscando noticias geopoliticas...") if _geo_refresh else st.empty():
    _geo_data   = _load_geo_feed(force_api=_geo_refresh)

_geo_alerts = _geo_data.get("alerts", [])
_geo_ts_raw = _geo_data.get("ts", "")
_geo_source = _geo_data.get("source", "cache")

# Timestamp legível
_geo_ts = None
if _geo_ts_raw:
    try:
        _geo_dt = datetime.datetime.fromisoformat(_geo_ts_raw)
        _geo_ts = _geo_dt.strftime("%d/%m %H:%M")
    except Exception:
        _geo_ts = _geo_ts_raw[:16]

if _geo_alerts:
    _cards_html = ""
    for _a in _geo_alerts[:10]:
        _impact  = _a.get("impact", "media")
        _cls     = _IMPACT_CLS.get(_impact, "media")
        _ilabel  = _IMPACT_ICON.get(_impact, _impact.upper())
        _comms   = [c for c in (_a.get("commodities") or ["global"]) if c]
        _is_glob = (_comms == ["global"] or not _comms)

        _comm_html = "".join(
            '<span class="geo-comm-tag">' + _COMM_DISPLAY.get(c, c.upper()) + '</span>'
            for c in (_comms if not _is_glob else ["global"])
        )

        # Usa headline traduzida se disponível, senão original
        _hl_raw   = (_a.get("headline_ptbr") or _a.get("headline") or "")
        _headline = _hl_raw.replace("<", "&lt;").replace(">", "&gt;")
        _desc_raw = (_a.get("description_ptbr") or _a.get("description") or "")[:160]
        _desc     = _desc_raw.replace("<", "&lt;").replace(">", "&gt;")
        _src      = (_a.get("source") or "")[:24]
        _date_raw = (_a.get("published_at") or "")
        _date     = _date_raw[:10] if _date_raw else ""
        _link     = (_a.get("link") or "").strip()
        _link_tag = (
            '<a class="geo-news-link" href="' + _link + '" target="_blank">&#8599;</a>'
            if _link else ""
        )
        _desc_html = (
            '<div class="geo-news-desc">' + _desc + '</div>'
            if _desc else ""
        )

        _cards_html += (
            '<div class="geo-news-card ' + _cls + '">'
            + _link_tag
            + '<div>'
            + '<span class="geo-impact-pill ' + _cls + '">' + _ilabel + '</span>'
            + _comm_html
            + '</div>'
            + '<div class="geo-news-headline">' + _headline + '</div>'
            + _desc_html
            + '<div class="geo-news-footer">'
            + '<span class="geo-news-source">' + _src + '</span>'
            + '<span class="geo-news-date">' + _date + '</span>'
            + '</div>'
            + '</div>'
        )

    _src_label = "API ao vivo" if _geo_source == "api" else "Celery cache"
    _meta_txt  = (
        ("Varredura: " + _geo_ts + " · " if _geo_ts else "")
        + "NewsData.io · " + str(len(_geo_alerts)) + " alertas · " + _src_label
    )

    st.markdown(
        '<div class="geo-feed-wrap">'
        + '<div class="geo-feed-meta" style="margin-bottom:8px">' + _meta_txt + '</div>'
        + '<div class="geo-feed-scroll">' + _cards_html + '</div>'
        + '</div>',
        unsafe_allow_html=True,
    )

else:
    # Estado vazio
    if _geo_source == "quota_exceeded":
        _empty_msg = "⚠️ Quota NewsData.io esgotada (HTTP 429) — créditos resetam à meia-noite UTC. Tente novamente amanhã ou atualize o plano."
    elif _geo_source == "no_key":
        _empty_msg = "Configure NEWSDATA_KEY_1 e NEWSDATA_KEY_2 no .env e clique em Refresh"
    elif _geo_ts:
        _empty_msg = "Nenhum alerta de alto impacto detectado · varredura " + _geo_ts
    else:
        _empty_msg = "Clique em Refresh para buscar noticias agora"

    st.markdown(
        '<div class="geo-feed-empty">'
        + '<div class="geo-feed-empty-dot"></div>'
        + '<span>' + _empty_msg + '</span>'
        + '</div>',
        unsafe_allow_html=True,
    )

st.markdown("<div style='margin-bottom:4px'></div>", unsafe_allow_html=True)

# ========================
# ABAS
# ========================
abas = st.tabs(["Visao Geral", "Formação de Preço", "Pipeline", "Arbitragem", "Book", "Agentes", "Follow-ups 📋", "Compliance 🔏", "Samba Assistant 🤖", "Base de Conhecimento 🧠"])

# ─────────────────────────────────────────────────────────────────
# ABA 1 — VISAO GERAL
# ─────────────────────────────────────────────────────────────────
with abas[0]:
    _cambio = _get_cambio(_ov)

    # ── Pipeline recente — cards compactos ───────────────────────
    st.markdown('<div class="section-title">NEGOCIAÇÕES ATIVAS — ÚLTIMOS 15</div>', unsafe_allow_html=True)
    df_deals = load_deals_recentes()
    if df_deals.empty:
        st.info("Nenhum deal ativo com direção definida (BID/ASK).")
    else:
        _dir_color = {"BID": "#326496", "ASK": "#fa3232"}
        rows_html = ""
        for _, dr in df_deals.iterrows():
            dc = _dir_color.get(str(dr.get("Dir", "")), "var(--samba-dim)")
            rows_html += (
                f'<tr>'
                f'<td style="color:var(--samba-ice);font-weight:600">{str(dr["Deal"])[:42]}</td>'
                f'<td style="color:var(--samba-dim)">{dr["Commodity"]}</td>'
                f'<td><span style="color:{dc};font-weight:700;font-size:11px">{dr["Dir"]}</span></td>'
                f'<td style="color:var(--samba-gold);font-family:monospace">{dr["Preco"]}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{dr["Stage"]}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{dr["Grupo"]}</td>'
                f'<td style="color:#555;font-size:11px">{dr["Data"]}</td>'
                f'</tr>'
            )
        st.markdown(f"""
        <div class="samba-table-wrap">
        <table class="samba-table">
          <thead><tr>
            <th>Negociação</th><th>Commodity</th><th>Dir</th>
            <th>Preço</th><th>Stage</th><th>Grupo</th><th>Data</th>
          </tr></thead>
          <tbody>{rows_html}</tbody>
        </table>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom:16px'></div>", unsafe_allow_html=True)

    # ── Mercado Interno BR — colapsável para nao poluir ──────────
    with st.expander("📊 Mercado Interno (Preços Físicos BRL/Saca)", expanded=False):
        st.caption("Referência de praças físicas brasileiras — mercado doméstico. Não reflete preços de exportação.")

        # Diagnóstico: conta total de rows na tabela
        try:
            with engine.connect() as _dc:
                _total = _dc.execute(sqlalchemy.text("SELECT COUNT(*) FROM tb_preco_fisico_raw")).scalar()
                _url_tipo = str(engine.url).split("@")[-1][:30] if "@" in str(engine.url) else str(engine.url)[:30]
                st.caption(f"🔌 {_url_tipo} | {_total} registros físicos")
        except Exception as _de:
            st.caption(f"🔌 Erro DB: {_de}")

        def _query_pracas(prod: str) -> list:
            """Busca praças direto pelo engine principal do app (já conectado ao Supabase)."""
            try:
                with engine.connect() as _c:
                    rows = _c.execute(sqlalchemy.text("""
                        SELECT DISTINCT ON (cidade, uf)
                            cidade || '/' || uf AS praca,
                            CASE WHEN produto='SUGAR'
                                THEN ROUND((preco_brl_ton*50/1000)::numeric,2)
                                ELSE ROUND((preco_brl_ton*60/1000)::numeric,2)
                            END AS preco_saca,
                            ROUND(preco_brl_ton::numeric,2) AS preco_ton,
                            fonte,
                            LEFT(timestamp::text,10) AS data
                        FROM tb_preco_fisico_raw
                        WHERE UPPER(produto)=UPPER(:p)
                        ORDER BY cidade, uf, timestamp DESC
                        LIMIT 20
                    """), {"p": prod}).fetchall()
                return rows
            except Exception as _e:
                st.caption(f"⚠️ {prod}: {_e}")
                return []

        _pracas_data = {
            "SOY":   _query_pracas("SOY"),
            "CORN":  _query_pracas("CORN"),
            "SUGAR": _query_pracas("SUGAR"),
        }

        _prac_cols = st.columns(3)
        for _col, _prod, _label in [
            (_prac_cols[0], "SOY",   "SOJA"),
            (_prac_cols[1], "CORN",  "MILHO"),
            (_prac_cols[2], "SUGAR", "AÇÚCAR"),
        ]:
            with _col:
                st.markdown(f'<div style="font-size:11px;letter-spacing:2px;color:var(--samba-gold);font-weight:800;margin-bottom:8px">{_label}</div>', unsafe_allow_html=True)
                _rows_p = _pracas_data[_prod]
                if not _rows_p:
                    st.caption("Sem dados.")
                else:
                    for _rp in _rows_p[:8]:
                        # row: (praca, preco_saca, preco_ton, fonte, data)
                        _nm  = str(_rp[0])[:24]
                        _rs  = float(_rp[1] or 0)
                        _ton = float(_rp[2] or 0)
                        _usd = round(_ton / _cambio, 0) if _cambio > 0 else 0
                        _dt  = str(_rp[4] or "")[:10]
                        st.markdown(
                            f'<div class="praca-pill">'
                            f'<span class="praca-name">{_nm}</span>'
                            f'<span class="praca-rs">R$ {_rs:.0f}/sc</span>'
                            f'<span class="praca-usd">≈ USD {_usd:.0f}/t</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

# ─────────────────────────────────────────────────────────────────
# ABA 2 — FORMAÇÃO DE PREÇO (Quick Quote → Calculadora → Cotação PDF)
# ─────────────────────────────────────────────────────────────────
with abas[1]:
    from dashboards.pricing_widget import render_pricing_tab
    render_pricing_tab()

# ─────────────────────────────────────────────────────────────────
# ABA 3 — PIPELINE KANBAN
# ─────────────────────────────────────────────────────────────────
with abas[2]:
    st.markdown('<div class="section-title">PIPELINE COMERCIAL — VISÃO INTEGRADA</div>', unsafe_allow_html=True)
    st.caption("Fonte primária: planilha Google Sheets (todos andamento + Andamento Vietnã). Enriquecido com docs Drive.")

    # ── Carrega dados ──────────────────────────────────────────────
    _pipe_deals = _load_planilha_pipeline()
    _inventory  = load_doc_inventory()
    _comp_seals = load_compliance_seals()

    if not _pipe_deals:
        st.warning("Não foi possível carregar a planilha. Verifique autenticação Google.")
    else:
        # ── KPIs rápidos ──────────────────────────────────────────
        _p_total  = len(_pipe_deals)
        _p_viet   = sum(1 for d in _pipe_deals if d["fonte"].startswith("🇻🇳"))
        _p_negoc  = sum(1 for d in _pipe_deals if "negoci" in d["status_raw"].lower() or d["status_raw"].lower() == "em deal")
        _p_aguard = sum(1 for d in _pipe_deals if "pendente" in d["status_raw"].lower())

        _pk1, _pk2, _pk3, _pk4 = st.columns(4)
        _kpi_s = "background:var(--samba-bg-card);border:1px solid var(--samba-line);border-radius:8px;padding:14px 12px;text-align:center"
        with _pk1:
            st.markdown(f'<div style="{_kpi_s}"><div style="font-size:24px;font-weight:800;color:var(--samba-gold)">{_p_total}</div><div style="font-size:10px;color:var(--samba-dim);margin-top:3px">DEALS ATIVOS</div></div>', unsafe_allow_html=True)
        with _pk2:
            st.markdown(f'<div style="{_kpi_s}"><div style="font-size:24px;font-weight:800;color:#329632">{_p_negoc}</div><div style="font-size:10px;color:var(--samba-dim);margin-top:3px">EM NEGOCIAÇÃO</div></div>', unsafe_allow_html=True)
        with _pk3:
            st.markdown(f'<div style="{_kpi_s}"><div style="font-size:24px;font-weight:800;color:#fa8200">{_p_aguard}</div><div style="font-size:10px;color:var(--samba-dim);margin-top:3px">AGUARDANDO</div></div>', unsafe_allow_html=True)
        with _pk4:
            st.markdown(f'<div style="{_kpi_s}"><div style="font-size:24px;font-weight:800;color:#64c8fa">{_p_viet}</div><div style="font-size:10px;color:var(--samba-dim);margin-top:3px">🇻🇳 VIETNÃ</div></div>', unsafe_allow_html=True)

        st.markdown("<div style='margin:14px 0 6px'></div>", unsafe_allow_html=True)

        # ── Filtros ───────────────────────────────────────────────
        _pf1, _pf2, _pf3 = st.columns(3)
        with _pf1:
            _all_grupos = sorted({d["grupo"].title() for d in _pipe_deals if d["grupo"]})
            _sel_grupo = st.selectbox("Parceiro / Grupo", ["Todos"] + _all_grupos, key="_pipe_grupo")
        with _pf2:
            _all_prods  = sorted({d["produto"].title() for d in _pipe_deals if d["produto"]})
            _sel_prod   = st.selectbox("Produto", ["Todos"] + _all_prods, key="_pipe_prod")
        with _pf3:
            _fonte_opts = ["Todas", "🇧🇷 Andamento", "🇻🇳 Vietnã"]
            _sel_fonte  = st.selectbox("Fonte", _fonte_opts, key="_pipe_fonte")

        _pipe_filtered = [
            d for d in _pipe_deals
            if (_sel_grupo == "Todos" or d["grupo"].title() == _sel_grupo)
            and (_sel_prod  == "Todos" or d["produto"].title() == _sel_prod)
            and (_sel_fonte == "Todas" or d["fonte"] == _sel_fonte)
        ]

        # ── Renderiza por parceiro ────────────────────────────────
        st.markdown("<div style='margin-top:10px'></div>", unsafe_allow_html=True)

        _grupos_render = {}
        for d in _pipe_filtered:
            _grupos_render.setdefault(d["grupo"] or "—", []).append(d)

        for _grup_key, _grup_deals in _grupos_render.items():
            _is_viet = any(d["fonte"].startswith("🇻🇳") for d in _grup_deals)
            _grup_color = "#64c8fa" if _is_viet else "var(--samba-gold)"
            _fonte_tag  = "🇻🇳 Câmara Brasil-Vietnã" if _is_viet else f"{len(_grup_deals)} deal{'s' if len(_grup_deals)>1 else ''}"

            st.markdown(
                f'<div style="display:flex;align-items:center;gap:10px;margin:16px 0 6px">'
                f'<div style="font-size:11px;font-weight:800;letter-spacing:2px;color:{_grup_color}">'
                f'{_grup_key.upper()}</div>'
                f'<div style="font-size:10px;color:var(--samba-dim)">{_fonte_tag}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

            _rows_html = ""
            for d in _grup_deals:
                _emoji    = _prod_emoji(d["produto"])
                _prod_lbl = (d["produto"] or "—").title()
                _tipo_lbl = "📥 Compra" if "pedido" in (d["tipo"] or "").lower() else "📤 Venda"
                # Sanitiza quebras de linha antes de embutir em HTML
                _vis_raw  = (d["vis_rapida"] or d["especificacao"] or "—")
                _vis      = _vis_raw.replace("\r", "").replace("\n", " ")[:90]
                _vis_ttl  = _vis_raw.replace("\r", "").replace("\n", " ").replace('"', "&quot;")
                _sit      = (d["situacao"] or "").replace("\r", "").replace("\n", " ")[:70]
                _comp_lbl = (d["comprador"] or "—")[:35]
                _forn_lbl = (d["fornecedor"] or "—")[:30]
                _badge    = _status_badge_html(d["status_raw"], small=True)
                _doc_html = render_doc_badges(_grup_key, _inventory)
                _job_lbl  = d["job"]
                _data_lbl = d["data"] or "—"

                _rows_html += (
                    f'<tr>'
                    f'<td style="color:var(--samba-dim);font-size:10px;white-space:nowrap">{_job_lbl}<br><span style="color:#555">{_data_lbl}</span></td>'
                    f'<td style="font-size:12px">{_emoji} <span style="color:var(--samba-ice);font-weight:600">{_prod_lbl}</span></td>'
                    f'<td style="font-size:11px;color:var(--samba-dim)">{_tipo_lbl}</td>'
                    f'<td style="color:var(--samba-gold);font-size:11px;max-width:220px;overflow:hidden;text-overflow:ellipsis" title="{_vis_ttl}">{_vis}</td>'
                    f'<td style="color:var(--samba-dim);font-size:11px">{_comp_lbl}</td>'
                    f'<td style="color:var(--samba-dim);font-size:11px">{_forn_lbl}</td>'
                    f'<td>{_badge}</td>'
                    f'<td style="font-size:10px;color:var(--samba-dim)">{_doc_html}</td>'
                    f'</tr>'
                )
                if _sit:
                    _rows_html += (
                        f'<tr style="background:rgba(0,0,0,0.2)">'
                        f'<td></td>'
                        f'<td colspan="7" style="font-size:10px;color:#666;padding-bottom:6px;padding-top:2px">'
                        f'↳ {_sit}</td></tr>'
                    )

            st.markdown(
                f'<div class="samba-table-wrap">'
                f'<table class="samba-table"><thead><tr>'
                f'<th style="width:90px">JOB / Data</th>'
                f'<th>Produto</th><th>Tipo</th>'
                f'<th>Resumo</th><th>Comprador</th><th>Fornecedor</th>'
                f'<th>Status</th><th>Docs</th>'
                f'</tr></thead><tbody>{_rows_html}</tbody></table></div>',
                unsafe_allow_html=True,
            )

        # ── Secao Declinados / Arquivados ────────────────────────────
        st.markdown("<div style='margin-top:24px'></div>", unsafe_allow_html=True)
        _decl_all = _load_declinados()
        _decl_label = f"🗂️ Histórico / Arquivados — Declinados ({len(_decl_all)} deals)"
        with st.expander(_decl_label, expanded=False):
            if not _decl_all:
                st.info("Aba 'Declinados' vazia ou inacessivel.")
            else:
                st.caption(
                    "Deals que nao foram levados adiante. Muitos tem contexto rico e "
                    "comprador real — util para reativar ou comparar precos."
                )
                # Filtro de busca dentro dos declinados
                _decl_search = st.text_input(
                    "Buscar em Declinados",
                    placeholder="ex: soja, Mexico, Brako, ICUMSA...",
                    key="_decl_search",
                )
                _decl_prod_opts = sorted({d["produto"].title() for d in _decl_all if d["produto"]})
                _decl_prod_sel  = st.selectbox(
                    "Filtrar por Produto",
                    ["Todos"] + _decl_prod_opts,
                    key="_decl_prod",
                )

                _decl_filtered = _decl_all
                if _decl_prod_sel != "Todos":
                    _decl_filtered = [d for d in _decl_filtered if d["produto"].title() == _decl_prod_sel]
                if _decl_search:
                    _dq = _decl_search.lower()
                    _decl_filtered = [
                        d for d in _decl_filtered
                        if _dq in (d.get("produto") or "").lower()
                        or _dq in (d.get("grupo") or "").lower()
                        or _dq in (d.get("comprador") or "").lower()
                        or _dq in (d.get("fornecedor") or "").lower()
                        or _dq in (d.get("vis_rapida") or "").lower()
                        or _dq in (d.get("situacao") or "").lower()
                    ]

                st.caption(f"{len(_decl_filtered)} resultado(s)")

                _decl_rows_html = ""
                for d in _decl_filtered[:100]:
                    _emoji    = _prod_emoji(d["produto"])
                    _prod_lbl = (d["produto"] or "—").title()
                    _vis_raw  = (d["vis_rapida"] or d["especificacao"] or "—")
                    _vis      = _vis_raw.replace("\r","").replace("\n"," ")[:90]
                    _vis_ttl  = _vis_raw.replace("\r","").replace("\n"," ").replace('"',"&quot;")
                    _sit      = (d["situacao"] or "").replace("\r","").replace("\n"," ")[:70]
                    _comp_lbl = (d["comprador"] or "—")[:35]
                    _forn_lbl = (d["fornecedor"] or "—")[:30]
                    _job_lbl  = d["job"]
                    _data_lbl = d["data"] or "—"
                    _grupo_lbl = (d["grupo"] or "—")[:30]
                    _badge    = _status_badge_html(d["status_raw"], small=True)

                    _decl_rows_html += (
                        f'<tr style="opacity:0.72">'
                        f'<td style="color:var(--samba-dim);font-size:10px;white-space:nowrap">'
                        f'{_job_lbl}<br><span style="color:#444">{_data_lbl}</span></td>'
                        f'<td style="color:var(--samba-dim);font-size:11px">{_grupo_lbl}</td>'
                        f'<td style="font-size:12px">{_emoji} <span style="color:#aaa;font-weight:600">{_prod_lbl}</span></td>'
                        f'<td style="color:#666;font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis" title="{_vis_ttl}">{_vis}</td>'
                        f'<td style="color:#666;font-size:11px">{_comp_lbl}</td>'
                        f'<td style="color:#666;font-size:11px">{_forn_lbl}</td>'
                        f'<td>{_badge}</td>'
                        f'</tr>'
                    )
                    if _sit:
                        _decl_rows_html += (
                            f'<tr style="background:rgba(0,0,0,0.15);opacity:0.65">'
                            f'<td></td><td></td>'
                            f'<td colspan="5" style="font-size:10px;color:#555;padding-bottom:4px;padding-top:2px">'
                            f'↳ {_sit}</td></tr>'
                        )

                st.markdown(
                    f'<div class="samba-table-wrap">'
                    f'<table class="samba-table"><thead><tr>'
                    f'<th style="width:80px">JOB / Data</th>'
                    f'<th>Parceiro</th><th>Produto</th>'
                    f'<th>Resumo</th><th>Comprador</th><th>Fornecedor</th>'
                    f'<th>Status</th>'
                    f'</tr></thead><tbody>{_decl_rows_html}</tbody></table></div>',
                    unsafe_allow_html=True,
                )
                if len(_decl_filtered) > 100:
                    st.caption(f"Exibindo primeiros 100 de {len(_decl_filtered)}. Use o filtro para refinar.")

# ─────────────────────────────────────────────────────────────────
# ABA 4 — ARBITRAGEM
# ─────────────────────────────────────────────────────────────────
with abas[3]:
    df_arb = load_arbitragem()

    if df_arb.empty:
        st.info("Nenhum deal com preco valido encontrado.")
    else:
        # Resumo por commodity
        st.markdown('<div class="section-title">RESUMO POR COMMODITY</div>', unsafe_allow_html=True)

        summary = (
            df_arb.groupby("Commodity")
            .agg(Deals=("Preco","count"), Min=("Preco","min"),
                 Med=("Preco","mean"), Max=("Preco","max"))
            .reset_index().sort_values("Deals", ascending=False)
        )
        summary.columns = ["Commodity", "Qtd", "Preco Min", "Preco Med", "Preco Max"]
        for col in ["Preco Min", "Preco Med", "Preco Max"]:
            summary[col] = summary[col].map("{:.2f}".format)

        st.dataframe(summary, width="stretch", hide_index=True)
        st.markdown("<hr style='border-color:rgba(255,255,255,0.05);margin:16px 0'>", unsafe_allow_html=True)

        # Filtros
        col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
        with col_f1:
            comm_list = ["Todas"] + sorted(df_arb["Commodity"].dropna().unique().tolist())
            filtro_comm = st.selectbox("Commodity", comm_list, key="arb_comm")
        with col_f2:
            dir_list = ["Todas"] + sorted(df_arb["Direcao"].dropna().unique().tolist())
            filtro_dir = st.selectbox("Direcao", dir_list, key="arb_dir")
        with col_f3:
            moeda_list = ["Todas"] + sorted(df_arb["Moeda"].dropna().unique().tolist())
            filtro_moeda = st.selectbox("Moeda", moeda_list, key="arb_moeda")

        df_f = df_arb.copy()
        if filtro_comm  != "Todas": df_f = df_f[df_f["Commodity"] == filtro_comm]
        if filtro_dir   != "Todas": df_f = df_f[df_f["Direcao"]   == filtro_dir]
        if filtro_moeda != "Todas": df_f = df_f[df_f["Moeda"]     == filtro_moeda]

        ask_d = df_f[df_f["Direcao"] == "ASK"]
        bid_d = df_f[df_f["Direcao"] == "BID"]
        unk_d = df_f[~df_f["Direcao"].isin(["ASK","BID"])]

        st.markdown(
            f'<div class="section-title" style="margin-top:8px">DEALS COM PRECO ({len(df_f)} registros)</div>',
            unsafe_allow_html=True,
        )
        m1, m2 = st.columns(2)
        m1.metric("ASK (Vendas)",  len(ask_d), f"Med: {ask_d['Preco'].mean():.2f}" if not ask_d.empty else None)
        m2.metric("BID (Compras)", len(bid_d), f"Med: {bid_d['Preco'].mean():.2f}" if not bid_d.empty else None)

        st.markdown("<div style='margin:8px 0'></div>", unsafe_allow_html=True)

        # Tabela premium — sem Originador/Grupo bruto, usa Cliente e Porto
        _arb_cols = [c for c in ["Commodity","Direcao","Preco","Moeda","Volume","Unid","Incoterm","Cliente","Porto"] if c in df_f.columns]
        _arb_show = df_f[_arb_cols].copy() if _arb_cols else df_f
        _arb_html = ""
        for _, _ar in _arb_show.iterrows():
            _dc = "#326496" if str(_ar.get("Direcao","")) == "BID" else "#fa3232"
            _arb_html += (
                f'<tr>'
                f'<td style="color:var(--samba-ice);font-weight:600">{_ar.get("Commodity","")}</td>'
                f'<td><span style="color:{_dc};font-weight:700;font-size:11px">{_ar.get("Direcao","")}</span></td>'
                f'<td style="color:var(--samba-gold);font-family:monospace">{float(_ar.get("Preco",0)):.2f}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{_ar.get("Moeda","USD")}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{_ar.get("Volume","") or "-"}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{_ar.get("Unid","MT")}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{_ar.get("Incoterm","-")}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{_ar.get("Cliente","-")}</td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{_ar.get("Porto","-")}</td>'
                f'</tr>'
            )
        st.markdown(f"""
        <div class="samba-table-wrap">
        <table class="samba-table">
          <thead><tr>
            <th>Commodity</th><th>Dir</th><th>Preço USD/MT</th>
            <th>Moeda</th><th>Volume</th><th>Unid</th>
            <th>Incoterm</th><th>Cliente</th><th>Porto</th>
          </tr></thead>
          <tbody>{_arb_html}</tbody>
        </table>
        </div>
        """, unsafe_allow_html=True)

        # Cruzamento ASK x BID
        if not ask_d.empty and not bid_d.empty:
            st.markdown("<hr style='border-color:rgba(255,255,255,0.05);margin:16px 0'>", unsafe_allow_html=True)
            st.markdown('<div class="section-title">CRUZAMENTO DE OPORTUNIDADES (ASK x BID)</div>', unsafe_allow_html=True)

            MIN_SPREAD = {
                "soja": 3.0, "farelo": 3.0, "milho": 3.0, "trigo": 3.0,
                "arroz": 3.0, "acucar": 3.0, "etanol": 0.05, "algodao": 5.0,
                "cafe": 8.0, "cacau": 8.0, "frango": 5.0, "boi": 5.0,
                "oleo": 10.0, "chicken": 50.0, "pork": 20.0, "beef": 50.0,
            }

            oportunidades = []
            for _, ask in ask_d.iterrows():
                for _, bid in bid_d.iterrows():
                    ca = (ask["Commodity"] or "").lower()
                    cb = (bid["Commodity"] or "").lower()
                    # Match por prefixo (soja, milho, etc.)
                    if not any(kw in ca and kw in cb for kw in ["soja","milho","arroz","acucar","algodao","cafe","oleo","chicken","pork","beef","frango"]):
                        if ca != cb:
                            continue
                    if ask["Moeda"] != bid["Moeda"]:
                        continue
                    spread = bid["Preco"] - ask["Preco"]
                    min_s = next((v for k, v in MIN_SPREAD.items() if k in ca), 3.0)
                    if spread >= min_s:
                        oportunidades.append({
                            "Commodity":  ask["Commodity"],
                            "Vendedor":   ask.get("Cliente", ask.get("Originador", "-")),
                            "Preco ASK":  ask["Preco"],
                            "Comprador":  bid.get("Cliente", bid.get("Originador", "-")),
                            "Preco BID":  bid["Preco"],
                            "Spread":     round(spread, 2),
                            "Moeda":      ask["Moeda"],
                        })

            if oportunidades:
                df_oport = pd.DataFrame(oportunidades).sort_values("Spread", ascending=False)
                st.success(f"🎯 {len(df_oport)} oportunidade(s) detectada(s)!")
                st.dataframe(
                    df_oport, width="stretch", hide_index=True,
                    column_config={
                        "Preco ASK": st.column_config.NumberColumn("Preco ASK", format="%.2f"),
                        "Preco BID": st.column_config.NumberColumn("Preco BID", format="%.2f"),
                        "Spread":    st.column_config.NumberColumn("Spread", format="%.2f", width="small"),
                    },
                )
            else:
                st.info("Nenhuma oportunidade no momento. Classifique mais deals como ASK/BID.")

# ─────────────────────────────────────────────────────────────────
# ABA 5 — BOOK
# ─────────────────────────────────────────────────────────────────
with abas[4]:
    st.markdown('<div class="section-title">ORDER BOOK — SPREAD POR COMMODITY</div>', unsafe_allow_html=True)
    st.caption("Melhores preços confirmados (BID/ASK) com direção definida. Preços > USD 6.000/MT são excluídos por sanidade.")

    df_book = load_book()

    if df_book.empty:
        st.info("Sem dados de book. Os deals precisam ter direção BID ou ASK e preço definido.")
    else:
        df_book = df_book.copy()
        df_book["Spread"] = (df_book["Best BID"] - df_book["Best ASK"]).round(2)

        # Renderiza como cards por commodity — muito mais legivel que tabela
        _bk_cols = st.columns(min(len(df_book), 4))
        for _bk_i, (_bk_c, _bk_row) in enumerate(zip(
            (_bk_cols * ((len(df_book) // len(_bk_cols)) + 1))[:len(df_book)],
            df_book.itertuples()
        )):
            _ask = getattr(_bk_row, "Best_ASK", None)  # pandas renomeia
            _bid = getattr(_bk_row, "Best_BID", None)
            # fallback para acesso por indice
            try:
                _ask = float(df_book.at[_bk_i, "Best ASK"]) if pd.notna(df_book.at[_bk_i, "Best ASK"]) else None
                _bid = float(df_book.at[_bk_i, "Best BID"]) if pd.notna(df_book.at[_bk_i, "Best BID"]) else None
                _spr = float(df_book.at[_bk_i, "Spread"])   if pd.notna(df_book.at[_bk_i, "Spread"])   else None
            except Exception:
                _ask, _bid, _spr = None, None, None
            _spr_cor = "#329632" if (_spr or 0) > 0 else "#fa3232" if (_spr or 0) < 0 else "#555"
            _ask_s = f"USD {_ask:.2f}" if _ask else "—"
            _bid_s = f"USD {_bid:.2f}" if _bid else "—"
            _spr_s = f"{(_spr or 0):+.2f}" if _spr is not None else "—"
            with _bk_c:
                st.markdown(f"""
                <div class="samba-card" style="text-align:center;padding:16px 12px">
                  <div style="font-size:10px;letter-spacing:2px;color:var(--samba-dim);font-weight:700;margin-bottom:10px">{str(df_book.at[_bk_i,'Commodity']).upper()}</div>
                  <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                    <div>
                      <div style="font-size:9px;color:var(--samba-dim);letter-spacing:1px">BEST ASK</div>
                      <div style="color:#fa3232;font-family:monospace;font-weight:700;font-size:13px">{_ask_s}</div>
                    </div>
                    <div>
                      <div style="font-size:9px;color:var(--samba-dim);letter-spacing:1px">BEST BID</div>
                      <div style="color:#326496;font-family:monospace;font-weight:700;font-size:13px">{_bid_s}</div>
                    </div>
                  </div>
                  <div style="border-top:1px solid var(--samba-line);padding-top:8px;margin-top:4px">
                    <span style="font-size:10px;color:var(--samba-dim)">SPREAD </span>
                    <span style="color:{_spr_cor};font-family:monospace;font-weight:700">{_spr_s}</span>
                    <span style="font-size:10px;color:#555;margin-left:8px">{int(df_book.at[_bk_i,'Deals'])} deal(s)</span>
                  </div>
                </div>
                """, unsafe_allow_html=True)

    # Documentos gerados — overview do inventario Drive
    st.markdown("<div style='margin:20px 0 8px'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">DOCUMENTOS GERADOS — INVENTÁRIO DRIVE</div>', unsafe_allow_html=True)

    _inv_book = load_doc_inventory()
    _inv_stats_book = _inv_book.get("stats", {})
    _inv_by_client = _inv_book.get("by_client", {})
    _inv_sheet_id = _inv_book.get("sheet_id")
    _inv_gen_book = (_inv_book.get("generated_at") or "")[:16].replace("T", " ")

    if not _inv_stats_book:
        st.info("Sem snapshot de inventário. Execute scripts/scan_drive_inventory.py para gerar.")
    else:
        # KPIs do inventario
        _doc_types_order = ["LOI", "ICPO", "SCO", "FCO", "SPA", "SBLC", "RWA", "Ficha de Cadastro"]
        _kpi_cols = st.columns(len(_doc_types_order))
        for _ki, _kt in enumerate(_doc_types_order):
            _kv = _inv_stats_book.get(_kt, 0)
            with _kpi_cols[_ki]:
                st.markdown(f"""
                <div style="background:var(--samba-bg-card);border:1px solid var(--samba-line);
                     border-radius:10px;padding:10px 8px;text-align:center">
                  <div style="font-size:9px;letter-spacing:1.5px;color:var(--samba-dim);font-weight:700">{_kt}</div>
                  <div style="font-size:22px;font-weight:900;color:{'var(--samba-gold)' if _kv > 0 else '#333'}">{_kv}</div>
                </div>
                """, unsafe_allow_html=True)

        _noise = _inv_stats_book.get("Ruido", 0)
        _outro = _inv_stats_book.get("Outro", 0)
        st.markdown(
            f'<div style="font-size:11px;color:var(--samba-dim);margin:10px 0">'
            f'<span style="color:#555">{_noise} arquivos ruído (.xlsx/.html) &middot; '
            f'{_outro} não classificados &middot; '
            f'varredura: {_inv_gen_book}</span>'
            + (f' &middot; <a href="https://docs.google.com/spreadsheets/d/{_inv_sheet_id}/edit" '
               f'target="_blank" style="color:var(--samba-gold)">📊 Ver planilha completa</a>'
               if _inv_sheet_id else "")
            + "</div>",
            unsafe_allow_html=True,
        )

        # Top clientes com mais documentos
        if _inv_by_client:
            _top_clients = sorted(
                ((cl, len(docs)) for cl, docs in _inv_by_client.items()),
                key=lambda x: -x[1]
            )[:8]
            _cl_html = ""
            for _cn, _cd in _top_clients:
                _cl_html += (
                    f'<div class="praca-pill">'
                    f'<span class="praca-name">{_cn.title()}</span>'
                    f'<span class="praca-usd">{_cd} doc(s)</span>'
                    f'</div>'
                )
            _cl_cols = st.columns(2)
            _half = len(_top_clients) // 2 + len(_top_clients) % 2
            with _cl_cols[0]:
                st.markdown("".join(
                    f'<div class="praca-pill"><span class="praca-name">{_cn.title()}</span>'
                    f'<span class="praca-usd">{_cd} doc(s)</span></div>'
                    for _cn, _cd in _top_clients[:_half]
                ), unsafe_allow_html=True)
            with _cl_cols[1]:
                st.markdown("".join(
                    f'<div class="praca-pill"><span class="praca-name">{_cn.title()}</span>'
                    f'<span class="praca-usd">{_cd} doc(s)</span></div>'
                    for _cn, _cd in _top_clients[_half:]
                ), unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────
# ABA 6 — AGENTES
# ─────────────────────────────────────────────────────────────────
with abas[5]:
    st.markdown('<div class="section-title">STATUS DOS AGENTES</div>', unsafe_allow_html=True)

    AGENTES_CONFIG = [
        {
            "nome": "Manager Agent",
            "descricao": "Classifica BID/ASK, atribui socios e detecta arbitragem",
            "log_glob": "manager_agent*.jsonl",
        },
        {
            "nome": "Extractor Agent",
            "descricao": "Extrai commodities, preco e volume de mensagens brutas",
            "log_glob": "extractor_agent*.jsonl",
        },
        {
            "nome": "Market Agent",
            "descricao": "Scraping de mercado fisico e snapshots CBOT/ICE/FX",
            "log_glob": "market_agent*.jsonl",
        },
        {
            "nome": "Risk Agent",
            "descricao": "Score de risco por deal via IA",
            "log_glob": "risk_agent*.jsonl",
        },
        {
            "nome": "Report Agent",
            "descricao": "Gera relatorios PDF e resumos por socio",
            "log_glob": "report_agent*.jsonl",
        },
    ]

    ag_cols = st.columns(len(AGENTES_CONFIG))
    for i, ag in enumerate(AGENTES_CONFIG):
        last_run = _agent_last_run(ag["log_glob"])
        with ag_cols[i]:
            if last_run is None:
                status_cor  = "#555"
                status_txt  = "SEM LOG"
                last_str    = "Nunca executado"
            else:
                delta = datetime.datetime.now() - last_run
                mins  = int(delta.total_seconds() / 60)
                if mins < 10:
                    status_cor = "#00ff9c"
                    status_txt = "ATIVO"
                elif mins < 60:
                    status_cor = "#fa8200"
                    status_txt = "RECENTE"
                else:
                    status_cor = "#555"
                    status_txt = "OCIOSO"
                last_str = last_run.strftime("%d/%m %H:%M")

            st.markdown(f"""
            <div class="samba-card" style="text-align:center;">
                <div style="font-size:11px;letter-spacing:2px;color:#FA8200;font-weight:800;margin-bottom:10px;">
                    {ag['nome'].upper()}
                </div>
                <div style="font-size:22px;font-weight:900;color:{status_cor};margin-bottom:4px;">
                    {status_txt}
                </div>
                <div style="font-size:11px;color:#777;margin-bottom:8px;">{last_str}</div>
                <div style="font-size:11px;color:#999;line-height:1.5;">{ag['descricao']}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<hr style='border-color:rgba(255,255,255,0.05);margin:20px 0'>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">ATIVIDADE RECENTE DOS AGENTES</div>', unsafe_allow_html=True)

    import glob as _glob_mod
    import json as _json_log

    all_logs = sorted(_glob_mod.glob(str(ROOT / "data" / "logs" / "*.jsonl")), reverse=True)
    if not all_logs:
        st.info("Nenhum log encontrado em data/logs/.")
    else:
        _log_f1, _log_f2 = st.columns([3, 1])
        with _log_f1:
            log_sel = st.selectbox("Arquivo de log", [Path(p).name for p in all_logs], key="log_sel")
        with _log_f2:
            _log_limit = st.number_input("Últimas N linhas", min_value=10, max_value=500, value=50, step=10, key="log_limit")

        log_path = ROOT / "data" / "logs" / log_sel

        # Eventos a ocultar (ruido de atribuicao automatica sem valor para o usuario)
        _SKIP_EVENTS = {"deal_assigned", "assignments_done"}
        # Mapa de emojis por evento
        _EVENT_ICON = {
            "run_finished":        "✅",
            "briefing_generated":  "📋",
            "matches_detected":    "🎯",
            "matches_found":       "🎯",
            "deal_created":        "🆕",
            "deal_updated":        "✏️",
            "extraction_done":     "🔍",
            "sync_done":           "🔄",
            "error":               "❌",
        }

        def _parse_log_line(raw: str) -> dict | None:
            """Parse TSV: timestamp\\tagent\\tevent\\tlevel\\tjson_payload"""
            parts = raw.split("\t", 4)
            if len(parts) < 4:
                return None
            ts, agent, event, level = parts[0], parts[1], parts[2], parts[3]
            payload_raw = parts[4].strip() if len(parts) > 4 else ""
            # Remove aspas externas se presentes
            if payload_raw.startswith('"') and payload_raw.endswith('"'):
                payload_raw = payload_raw[1:-1].replace('""', '"')
            try:
                payload = _json_log.loads(payload_raw) if payload_raw else {}
            except Exception:
                payload = {"info": payload_raw[:120]} if payload_raw else {}
            if event in _SKIP_EVENTS:
                return None
            # Monta descricao legivel do payload
            desc_parts = []
            for k, v in payload.items():
                if k in ("deal_id", "run_count", "chars"):
                    continue
                if isinstance(v, list):
                    desc_parts.append(f"{k}: {', '.join(str(x) for x in v[:5])}")
                else:
                    desc_parts.append(f"{k}: {v}")
            desc = " | ".join(desc_parts)[:120]
            icon = _EVENT_ICON.get(event, "•")
            return {
                "ts":     ts[:16].replace("T", " "),
                "agente": agent,
                "evento": f"{icon} {event}",
                "nivel":  level,
                "detalhe": desc,
            }

        try:
            parsed_lines = []
            with open(log_path, "r", encoding="utf-8") as _f:
                raw_lines = [l.strip() for l in _f.readlines() if l.strip()]
            for ln in raw_lines[-int(_log_limit):]:
                p = _parse_log_line(ln)
                if p:
                    parsed_lines.append(p)

            if not parsed_lines:
                st.info("Nenhum evento relevante encontrado (eventos de atribuição são ocultados).")
            else:
                # Render como samba-table
                _log_html = ""
                for _lp in reversed(parsed_lines):  # mais recente primeiro
                    _lv_col = "#fa3232" if _lp["nivel"] == "ERROR" else "var(--samba-dim)"
                    _log_html += (
                        f'<tr>'
                        f'<td style="color:#555;font-family:monospace;font-size:11px;white-space:nowrap">{_lp["ts"]}</td>'
                        f'<td style="color:var(--samba-gold);font-size:11px;font-weight:600">{_lp["agente"]}</td>'
                        f'<td style="color:var(--samba-ice);font-size:12px">{_lp["evento"]}</td>'
                        f'<td style="color:{_lv_col};font-size:10px">{_lp["nivel"]}</td>'
                        f'<td style="color:var(--samba-dim);font-size:11px">{_lp["detalhe"]}</td>'
                        f'</tr>'
                    )
                st.markdown(f"""
                <div class="samba-table-wrap" style="max-height:380px;overflow-y:auto">
                <table class="samba-table">
                  <thead><tr>
                    <th>Hora</th><th>Agente</th><th>Evento</th><th>Nível</th><th>Detalhe</th>
                  </tr></thead>
                  <tbody>{_log_html}</tbody>
                </table>
                </div>
                """, unsafe_allow_html=True)
        except Exception as e:
            st.warning(f"Erro ao ler log: {e}")

# ─────────────────────────────────────────────────────────────────
# ABA 7 — FOLLOW-UPS
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def _load_followups():
    try:
        with engine.connect() as _conn:
            rows = _conn.execute(sqlalchemy.text("""
                SELECT
                    f.id,
                    f.deal_id,
                    d.name          AS deal_name,
                    d.commodity,
                    d.assignee,
                    d.source_group,
                    f.target_person,
                    f.target_group,
                    f.status,
                    f.due_at,
                    f.sent_at,
                    f.created_at,
                    f.response_received,
                    f.response_content,
                    f.message
                FROM followups f
                LEFT JOIN deals d ON d.id = f.deal_id
                ORDER BY
                    CASE f.status
                        WHEN 'pendente'   THEN 0
                        WHEN 'enviado'    THEN 1
                        WHEN 'respondido' THEN 2
                        WHEN 'expirado'   THEN 3
                        ELSE 4
                    END,
                    f.due_at ASC
                LIMIT 200
            """)).fetchall()
            return [dict(r._mapping) for r in rows]
    except Exception:
        return []


with abas[6]:
    st.markdown('<div class="section-title">FOLLOW-UPS — GESTÃO DE CONTATOS E COBRANÇAS</div>', unsafe_allow_html=True)
    st.caption("Agenda de cobranças enriquecida com dados da planilha (comprador, produto, responsável, contexto do deal).")

    _fu_rows   = _load_followups()
    _enrich_idx = _sheet_enrich_index()   # PROC V: grupo_norm → sheet deal

    def _fu_enrich(row: dict) -> dict:
        """Busca contexto da planilha para enriquecer o follow-up."""
        # Tenta match pelo source_group normalizado
        sg = _norm_grupo(row.get("source_group") or row.get("target_group") or "")
        match = _enrich_idx.get(sg) or {}
        # Fallback por deal_name (pode conter JOB code)
        if not match:
            dn = (row.get("deal_name") or "").lower()
            match = _enrich_idx.get(dn) or {}
        return match

    # ── KPIs ────────────────────────────────────────────────────────
    _fu_pendente   = sum(1 for r in _fu_rows if r["status"] == "pendente")
    _fu_enviado    = sum(1 for r in _fu_rows if r["status"] == "enviado")
    _fu_respondido = sum(1 for r in _fu_rows if r["status"] == "respondido")
    _fu_expirado   = sum(1 for r in _fu_rows if r["status"] == "expirado")

    _fu_k1, _fu_k2, _fu_k3, _fu_k4 = st.columns(4)
    with _fu_k1:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">AGUARDANDO ENVIO</div>
            <div class="kpi-value">{_fu_pendente}</div>
            <div class="kpi-sub">a despachar</div></div>""", unsafe_allow_html=True)
    with _fu_k2:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">ENVIADOS</div>
            <div class="kpi-value">{_fu_enviado}</div>
            <div class="kpi-sub">aguardando retorno</div></div>""", unsafe_allow_html=True)
    with _fu_k3:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">RESPONDIDOS</div>
            <div class="kpi-value" style="color:#329632">{_fu_respondido}</div>
            <div class="kpi-sub">parceiros retornaram</div></div>""", unsafe_allow_html=True)
    with _fu_k4:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">EXPIRADOS</div>
            <div class="kpi-value" style="color:#fa3232">{_fu_expirado}</div>
            <div class="kpi-sub">escalados ao gestor</div></div>""", unsafe_allow_html=True)

    st.markdown("<div style='margin-top:18px'></div>", unsafe_allow_html=True)

    # ── Filtros ──────────────────────────────────────────────────────
    _fuf1, _fuf2 = st.columns([2, 2])
    with _fuf1:
        _fu_filtro = st.selectbox(
            "Status",
            ["Todos", "pendente", "enviado", "respondido", "expirado"],
            key="_fu_filtro",
        )
    with _fuf2:
        _fu_search = st.text_input("Buscar (produto, comprador, parceiro...)", key="_fu_search", placeholder="ex: soja, eric, Brako...")

    _fu_filtered = _fu_rows if _fu_filtro == "Todos" else [r for r in _fu_rows if r["status"] == _fu_filtro]
    if _fu_search:
        _q = _fu_search.lower()
        _fu_filtered = [
            r for r in _fu_filtered
            if _q in (r.get("deal_name") or "").lower()
            or _q in (r.get("commodity") or "").lower()
            or _q in (r.get("target_person") or "").lower()
            or _q in (r.get("source_group") or "").lower()
        ]

    # ── Helpers visuais ──────────────────────────────────────────────
    def _fu_status_badge(s: str) -> str:
        _map = {
            "pendente":   ("#ffd700", "#1a1a00", "PENDENTE"),
            "enviado":    ("#5599ff", "#001440", "ENVIADO"),
            "respondido": ("#329632", "#0a1a08", "RESPONDIDO"),
            "expirado":   ("#fa3232", "#1a0808", "EXPIRADO"),
        }
        cor, bg, txt = _map.get(s, ("#666", "#111", s.upper()))
        return f'<span style="background:{bg};color:{cor};border:1px solid {cor}44;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700">{txt}</span>'

    def _fu_days_label(row: dict) -> str:
        ref = row.get("sent_at") or row.get("due_at") or row.get("created_at")
        if not ref:
            return "—"
        try:
            if isinstance(ref, str):
                ref = datetime.datetime.fromisoformat(ref)
            delta = (datetime.datetime.utcnow() - ref).days
            cor = "#fa3232" if delta > 7 else "#fa8200" if delta > 3 else "var(--samba-dim)"
            return f'<span style="color:{cor};font-weight:700;font-family:monospace">{delta}d</span>'
        except Exception:
            return "—"

    # ── Cards de follow-up ────────────────────────────────────────────
    if not _fu_filtered:
        st.info("Nenhum follow-up encontrado.")
    else:
        _fu_rows_html = ""
        for _fr in _fu_filtered:
            _sheet = _fu_enrich(_fr)

            # Identidade do deal — planilha tem prioridade
            _vis      = _sheet.get("vis_rapida") or _sheet.get("especificacao") or ""
            _job_code = _sheet.get("job") or ""
            _deal_id  = _job_code if _job_code and _job_code != "—" else (_fr.get("deal_name") or "—")
            _deal_id  = _deal_id[:48]

            # Produto — planilha tem prioridade
            _produto  = (_sheet.get("produto") or _fr.get("commodity") or "—").title()
            _emoji    = _prod_emoji(_sheet.get("produto") or _fr.get("commodity") or "")

            # Contato externo (quem vai receber o follow-up)
            _contato  = (_fr.get("target_person") or _sheet.get("comprador") or "—")[:40]

            # Responsável interno (quem cuida do deal na Samba)
            _resp     = (_sheet.get("responsavel") or _fr.get("assignee") or "—")[:30]
            # Limpeza: STATUS_AUTOMACAO não é responsável
            if _resp in ("PENDING_IA", "OK", "REJECTED", "SKIPPED") or not _resp.strip():
                _resp = _fr.get("assignee") or "—"

            # Status do deal na planilha
            _deal_status_badge = _status_badge_html(_sheet.get("status_raw", ""), small=True) if _sheet else ""

            # Status do follow-up
            _fu_badge = _fu_status_badge(_fr.get("status", ""))
            _dias_html = _fu_days_label(_fr)

            # Contexto da mensagem — sanitiza \n para nao quebrar HTML/Markdown
            def _s(t, n=80):
                return (t or "").replace("\r","").replace("\n"," ")[:n].replace("<","&lt;").replace(">","&gt;")
            _msg_prev  = _s(_fr.get("message"), 80)
            _resp_prev = _s(_fr.get("response_content"), 80)
            _vis_prev  = _s(_vis, 80)
            _vis_cell  = (f'<div style="color:#555;font-size:9px;margin-top:2px">{_vis_prev}</div>'
                          if _vis_prev else '')

            _fu_rows_html += (
                f'<tr>'
                f'<td style="min-width:130px">'
                f'<div style="color:var(--samba-gold);font-weight:700;font-size:11px">{_deal_id}</div>'
                f'{_vis_cell}'
                f'</td>'
                f'<td style="white-space:nowrap">{_emoji} <span style="color:var(--samba-ice);font-size:11px">{_produto}</span></td>'
                f'<td><div style="color:var(--samba-ice);font-size:11px">{_contato}</div></td>'
                f'<td style="color:var(--samba-dim);font-size:11px">{_resp}</td>'
                f'<td>{_fu_badge}</td>'
                f'<td>{_dias_html}</td>'
                f'<td style="color:var(--samba-dim);font-size:10px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_msg_prev or "—"}</td>'
                f'<td style="color:#329632;font-size:10px;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{_resp_prev or "—"}</td>'
                f'</tr>'
            )

        st.markdown(f"""
        <div class="samba-table-wrap">
        <table class="samba-table">
          <thead><tr>
            <th>Deal / Contexto</th>
            <th>Produto</th>
            <th>Contato Externo</th>
            <th>Responsável Samba</th>
            <th>Status Follow-up</th>
            <th>Aguardando</th>
            <th>Mensagem enviada</th>
            <th>Resposta recebida</th>
          </tr></thead>
          <tbody>{_fu_rows_html}</tbody>
        </table>
        </div>
        """, unsafe_allow_html=True)

    # ── Detalhes expansíveis de respostas ────────────────────────────
    _respondidos = [r for r in _fu_rows if r["status"] == "respondido" and r.get("response_content")]
    if _respondidos:
        st.markdown("<div style='margin-top:22px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-title">RESPOSTAS COMPLETAS — HISTÓRICO</div>', unsafe_allow_html=True)
        for _rr in _respondidos[:10]:
            _rr_sheet   = _fu_enrich(_rr)
            _rr_vis     = _rr_sheet.get("vis_rapida") or _rr_sheet.get("especificacao") or ""
            _rr_produto = (_rr_sheet.get("produto") or _rr.get("commodity") or "—").title()
            _rr_contato = _rr.get("target_person") or _rr_sheet.get("comprador") or "?"
            _rr_resp_int = _rr_sheet.get("responsavel") or _rr.get("assignee") or "—"
            _exp_title   = f"{_rr_produto} · {_rr_contato}"
            with st.expander(_exp_title):
                c_a, c_b = st.columns(2)
                with c_a:
                    st.markdown(f"**Produto:** {_rr_produto}")
                    st.markdown(f"**Contato externo:** {_rr_contato}")
                    st.markdown(f"**Responsável Samba:** {_rr_resp_int}")
                with c_b:
                    if _rr_vis:
                        st.markdown(f"**Contexto do deal:** {_rr_vis[:200]}")
                st.divider()
                st.markdown("**Mensagem enviada:**")
                st.code(_rr.get("message") or "—", language=None)
                st.markdown("**Resposta do parceiro:**")
                st.code(_rr.get("response_content") or "—", language=None)

# ─────────────────────────────────────────────────────────────────
# ABA 8 — COMPLIANCE DOCUMENTAL 🔏
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def _load_compliance_records() -> list[dict]:
    try:
        from models.database import DocumentCompliance, get_session
        sess = get_session()
        records = sess.query(DocumentCompliance).order_by(DocumentCompliance.audited_at.desc()).limit(200).all()
        sess.close()
        return [
            {
                "id": r.id,
                "deal_id": r.deal_id,
                "file_name": r.file_name or "—",
                "document_type": r.document_type or "—",
                "commodity": r.commodity or "—",
                "status": r.status or "—",
                "score": r.score or 0,
                "critical_issues": r.critical_issues or 0,
                "missing_clauses_count": r.missing_clauses_count or 0,
                "spec_divergences_count": r.spec_divergences_count or 0,
                "summary": r.summary or "",
                "report_json": r.report_json or "{}",
                "audited_at": r.audited_at.strftime("%d/%m/%Y %H:%M") if r.audited_at else "—",
            }
            for r in records
        ]
    except Exception:
        return []


with abas[7]:
    st.markdown('<div class="section-title">COMPLIANCE DOCUMENTAL — ICC/UCP 600</div>', unsafe_allow_html=True)
    st.caption("Auditoria de documentos contra clausulas obrigatorias ICC, UCP 600 (Art. 2, 14, 18, 20, 28), Incoterms 2020 e specs tecnicas de commodities.")

    # ── Carregar auditorias do banco ───────────────────────────────
    compliance_records = _load_compliance_records()

    # ── KPIs ───────────────────────────────────────────────────────
    n_total = len(compliance_records)
    n_verde   = sum(1 for r in compliance_records if r["status"] == "VERDE")
    n_amarelo = sum(1 for r in compliance_records if r["status"] == "AMARELO")
    n_vermelho = sum(1 for r in compliance_records if r["status"] == "VERMELHO")
    avg_score = (sum(r["score"] for r in compliance_records) / n_total) if n_total else 0

    kc1, kc2, kc3, kc4, kc5 = st.columns(5)
    with kc1:
        st.metric("Total Auditados", n_total)
    with kc2:
        st.metric("✅ Verde", n_verde)
    with kc3:
        st.metric("⚠️ Amarelo", n_amarelo)
    with kc4:
        st.metric("❌ Vermelho", n_vermelho)
    with kc5:
        st.metric("Score Medio", f"{avg_score:.0f}/100")

    st.markdown("<div style='margin:16px 0 8px 0'></div>", unsafe_allow_html=True)

    # ── Upload e auditoria ao vivo ─────────────────────────────────
    with st.expander("🔍 Auditar novo documento", expanded=False):
        uc1, uc2, uc3 = st.columns([2, 1, 1])
        with uc1:
            uploaded_file = st.file_uploader(
                "Carregar documento (PDF, DOCX, TXT)",
                type=["pdf", "docx", "txt", "md"],
                key="compliance_upload"
            )
        with uc2:
            doc_type_sel = st.selectbox(
                "Tipo de Documento",
                ["FCO", "LOI", "ICPO", "SPA", "NCNDA", "IMFPA"],
                key="compliance_doc_type"
            )
        with uc3:
            commodity_sel = st.selectbox(
                "Commodity (specs)",
                ["—", "soja_gmo", "soja_non_gmo", "acucar_icumsa45", "milho_amarelo", "oleo_soja_degomado"],
                key="compliance_commodity"
            )
        deal_id_input = st.number_input("Deal ID (opcional)", min_value=0, value=0, step=1, key="compliance_deal_id")

        if uploaded_file and st.button("▶ Auditar Agora", key="compliance_run"):
            import tempfile, os as _os
            _suffix = Path(uploaded_file.name).suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=_suffix) as tmp:
                tmp.write(uploaded_file.read())
                tmp_path = tmp.name
            try:
                from agents.documental_agent import DocumentalAgent as _DA
                _agent = _DA()
                _commodity = commodity_sel if commodity_sel != "—" else None
                _deal_id = int(deal_id_input) if deal_id_input > 0 else None
                with st.spinner("Auditando..."):
                    _report = _agent.auditar_documento(
                        file_path=tmp_path,
                        expected_type=doc_type_sel,
                        commodity=_commodity,
                        save_to_db=True,
                        deal_id=_deal_id,
                    )
                _os.unlink(tmp_path)
                st.cache_data.clear()

                # Exibe resultado inline
                _status_color = {"VERDE": "#329632", "AMARELO": "#fa8200", "VERMELHO": "#fa3232"}.get(_report.status.value, "#9a9aa0")
                st.markdown(
                    f'<div style="border:2px solid {_status_color};border-radius:12px;padding:16px;margin:8px 0">'
                    f'<span style="color:{_status_color};font-weight:800;font-size:18px">⬤ {_report.status.value}</span>'
                    f'&nbsp;&nbsp;<span style="color:#f5f5f7;font-weight:700">Score: {_report.score}/100</span><br>'
                    f'<span style="color:#9a9aa0;font-size:13px">{_report.summary}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                if _report.missing_clauses:
                    st.markdown("**Clausulas Ausentes:**")
                    for _c in _report.missing_clauses:
                        _col = "🔴" if _c.severity == "CRITICA" else "🟡"
                        st.markdown(f"{_col} **{_c.clause_name}** — `{_c.rule_citation}`")
                if _report.spec_divergences:
                    st.markdown("**Divergencias:**")
                    for _d in _report.spec_divergences:
                        _col = "🔴" if _d.severity == "CRITICA" else "🟡"
                        st.markdown(f"{_col} **{_d.field}**: encontrado `{_d.found}` | esperado `{_d.expected}` — `{_d.rule_citation}`")
                if _report.recommendations:
                    st.markdown("**Recomendacoes:**")
                    for _r in _report.recommendations[:5]:
                        st.markdown(f"• {_r}")
            except Exception as _exc:
                if '_os' in dir() and 'tmp_path' in dir():
                    try:
                        _os.unlink(tmp_path)
                    except Exception:
                        pass
                st.error(f"Erro na auditoria: {_exc}")

    # ── Tabela de auditorias ───────────────────────────────────────
    st.markdown('<div class="section-title" style="margin-top:16px">HISTORICO DE AUDITORIAS</div>', unsafe_allow_html=True)

    if not compliance_records:
        st.info("Nenhuma auditoria registrada ainda. Use o painel acima para auditar o primeiro documento.")
    else:
        # Filtros
        cf1, cf2, cf3 = st.columns(3)
        with cf1:
            status_filter = st.selectbox("Status", ["Todos", "VERDE", "AMARELO", "VERMELHO"], key="comp_status_f")
        with cf2:
            type_filter = st.selectbox("Tipo", ["Todos", "FCO", "LOI", "ICPO", "SPA", "NCNDA", "IMFPA"], key="comp_type_f")
        with cf3:
            comm_filter_c = st.selectbox(
                "Commodity",
                ["Todas"] + sorted(set(r["commodity"] for r in compliance_records if r["commodity"] != "—")),
                key="comp_comm_f"
            )

        filtered = compliance_records
        if status_filter != "Todos":
            filtered = [r for r in filtered if r["status"] == status_filter]
        if type_filter != "Todos":
            filtered = [r for r in filtered if r["document_type"] == type_filter]
        if comm_filter_c != "Todas":
            filtered = [r for r in filtered if r["commodity"] == comm_filter_c]

        # Render cards
        for rec in filtered[:50]:
            _sc = {"VERDE": "#329632", "AMARELO": "#fa8200", "VERMELHO": "#fa3232"}.get(rec["status"], "#9a9aa0")
            _icon = {"VERDE": "✅", "AMARELO": "⚠️", "VERMELHO": "❌"}.get(rec["status"], "•")
            _title = f'{_icon} {rec["document_type"]} — {rec["file_name"]} — Score {rec["score"]}/100'
            with st.expander(_title):
                ec1, ec2, ec3, ec4 = st.columns(4)
                ec1.metric("Status", rec["status"])
                ec2.metric("Score", f'{rec["score"]}/100')
                ec3.metric("Clausulas Ausentes", rec["missing_clauses_count"])
                ec4.metric("Issues Criticos", rec["critical_issues"])
                st.markdown(f"**Commodity:** {rec['commodity']}  |  **Deal ID:** {rec['deal_id'] or '—'}  |  **Auditado:** {rec['audited_at']}")
                st.caption(rec["summary"])
                try:
                    _rj = json.loads(rec["report_json"])
                    if _rj.get("missing_clauses"):
                        st.markdown("**Clausulas Ausentes:**")
                        for _c in _rj["missing_clauses"]:
                            _col2 = "🔴" if _c.get("severity") == "CRITICA" else "🟡"
                            st.markdown(f"{_col2} {_c.get('clause_name')} — `{_c.get('rule_citation','')}`")
                    if _rj.get("spec_divergences"):
                        st.markdown("**Divergencias:**")
                        for _d in _rj["spec_divergences"]:
                            _col2 = "🔴" if _d.get("severity") == "CRITICA" else "🟡"
                            st.markdown(
                                f"{_col2} {_d.get('field')}: `{_d.get('found')}` → `{_d.get('expected')}`  "
                                f"— `{_d.get('rule_citation','')}`"
                            )
                except Exception:
                    pass

# ─────────────────────────────────────────────────────────────────
# ABA 9 — SAMBA ASSISTANT (Conversational Hub com Tool Calling)
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def _load_pending_approvals() -> list[dict]:
    try:
        from models.database import PendingApproval, get_session
        sess = get_session()
        try:
            rows = (
                sess.query(PendingApproval)
                .filter(PendingApproval.status == "pending")
                .order_by(PendingApproval.created_at.desc())
                .limit(20)
                .all()
            )
            return [
                {
                    "id":           r.id,
                    "action_type":  r.action_type,
                    "description":  r.description or "",
                    "requested_by": r.requested_by or "Agente",
                    "created_at":   r.created_at.strftime("%d/%m %H:%M") if r.created_at else "—",
                }
                for r in rows
            ]
        finally:
            sess.close()
    except Exception:
        return []


with abas[8]:
    st.markdown('<div class="section-title">SAMBA ASSISTANT — CONVERSATIONAL HUB</div>', unsafe_allow_html=True)
    st.caption(
        "Conversa com o motor Gemini ligado ao ToolRegistry. Pode criar deals, "
        "mover stages e agendar follow-ups por voce."
    )

    import uuid as _uuid
    from services.conversation_store import (
        load_session as _load_session,
        append_turn as _append_turn,
        db_history_to_gemini as _db_to_gemini,
        persist_assistant_turn as _persist_assistant,
    )

    # ── Session state: id + historico em memoria ────────────────────
    if "samba_session_id" not in st.session_state:
        st.session_state["samba_session_id"] = f"sess_{_uuid.uuid4().hex[:12]}"
        # Hidratacao: nao ha historico antigo desta nova sessao.
        st.session_state["samba_messages"] = []
    else:
        # Recarrega do DB caso a pagina tenha sido remontada.
        if "samba_messages" not in st.session_state:
            st.session_state["samba_messages"] = _load_session(
                st.session_state["samba_session_id"]
            )

    _sess = st.session_state["samba_session_id"]

    # ── Controles de sessao ─────────────────────────────────────────
    col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([2, 1, 1])
    with col_ctrl1:
        st.text_input("Session ID", value=_sess, disabled=True, key="_samba_sid_view")
    with col_ctrl2:
        if st.button("🆕 Nova sessao", key="_samba_new"):
            st.session_state["samba_session_id"] = f"sess_{_uuid.uuid4().hex[:12]}"
            st.session_state["samba_messages"] = []
            st.rerun()
    with col_ctrl3:
        if st.button("🔄 Recarregar", key="_samba_reload"):
            st.session_state["samba_messages"] = _load_session(_sess)
            st.rerun()

    # ── Quick Actions ───────────────────────────────────────────────
    st.markdown(
        '<div style="font-size:10px;letter-spacing:1.5px;color:var(--samba-dim);'
        'font-weight:700;margin:10px 0 6px">AÇÕES RÁPIDAS</div>',
        unsafe_allow_html=True,
    )
    _qa1, _qa2, _qa3, _qa4, _qa5 = st.columns(5)
    _qa_inject = None   # texto que será injetado no chat input como se fosse digitado

    with _qa1:
        if st.button("🔍 Localizar Job", key="_qa_job", use_container_width=True):
            _qa_inject = "Liste todos os deals ativos no pipeline com JOB, produto e status."
    with _qa2:
        if st.button("📊 Resumo E-mails", key="_qa_email", use_container_width=True):
            _qa_inject = "Gere um resumo executivo dos últimos e-mails e extrações recebidas pelo sistema."
    with _qa3:
        if st.button("🌍 Geo News", key="_qa_geo", use_container_width=True):
            _qa_inject = "Há alertas geopolíticos relevantes para o nosso pipeline de commodities nas últimas 24h?"
    with _qa4:
        if st.button("📁 Auditar Drive", key="_qa_drive", use_container_width=True):
            _qa_inject = "Qual o status atual da base de conhecimento Drive? Quantos chunks, documentos e quando foi o último sync?"
    with _qa5:
        if st.button("⚠️ Pendências", key="_qa_pending", use_container_width=True):
            _qa_inject = "Liste todas as pendências e action items críticos ou altos em aberto."

    # ── Fila HITL — Aprovações pendentes ───────────────────────────
    _pending = _load_pending_approvals()
    if _pending:
        st.markdown(
            f'<div style="background:#1a0800;border:1px solid #fa820055;border-radius:10px;'
            f'padding:12px 16px;margin-bottom:12px">'
            f'<div style="font-size:10px;letter-spacing:1.5px;color:#fa8200;font-weight:800;margin-bottom:8px">'
            f'⏳ APROVAÇÕES PENDENTES ({len(_pending)})</div>',
            unsafe_allow_html=True,
        )
        for _pa in _pending:
            _hc1, _hc2, _hc3 = st.columns([3, 1, 1])
            with _hc1:
                st.markdown(
                    f'<div style="font-size:11px;color:var(--samba-ice)">'
                    f'<b>{_pa["action_type"]}</b> — {_pa["description"][:80]}'
                    f'<br><span style="color:#555;font-size:10px">{_pa["requested_by"]} · {_pa["created_at"]}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
            with _hc2:
                if st.button("✅ Aprovar", key=f"_hitl_ok_{_pa['id']}", use_container_width=True):
                    try:
                        from models.database import PendingApproval, get_session as _gs
                        import datetime as _dt
                        _s = _gs()
                        _row = _s.query(PendingApproval).filter_by(id=_pa["id"]).first()
                        if _row:
                            _row.status = "approved"
                            _row.approved_by = "painel"
                            _row.resolved_at = _dt.datetime.utcnow()
                            _s.commit()
                        _s.close()
                        _load_pending_approvals.clear()
                        st.rerun()
                    except Exception as _e:
                        st.error(f"Erro: {_e}")
            with _hc3:
                if st.button("❌ Rejeitar", key=f"_hitl_no_{_pa['id']}", use_container_width=True):
                    try:
                        from models.database import PendingApproval, get_session as _gs
                        import datetime as _dt
                        _s = _gs()
                        _row = _s.query(PendingApproval).filter_by(id=_pa["id"]).first()
                        if _row:
                            _row.status = "rejected"
                            _row.resolved_at = _dt.datetime.utcnow()
                            _s.commit()
                        _s.close()
                        _load_pending_approvals.clear()
                        st.rerun()
                    except Exception as _e:
                        st.error(f"Erro: {_e}")
        st.markdown("</div>", unsafe_allow_html=True)

    # ── DB-First lookup ─────────────────────────────────────────────
    def _db_quick_lookup(query: str) -> str | None:
        """
        Tenta responder a query diretamente via SQLite antes de chamar o LLM.
        Retorna resposta pronta ou None se precisar escalar para Gemini+RAG.

        Padrões detectados:
          - "job NNN" ou "2026NNN" → busca deal específico
          - "deals ativos" / "pipeline" → lista deals
          - "action items" / "pendências" → lista MeetingActionItem
          - "market" / "mercado" → último snapshot
        """
        import re as _re
        q_low = query.lower().strip()

        try:
            # ── Busca por JOB code ──────────────────────────────────
            job_match = _re.search(r"\b(20\d\d[A-Z]{2,8}\d{3,6}|job\s+[\w]+)\b", query, _re.IGNORECASE)
            if job_match:
                job_code = job_match.group(0).replace("job ", "").strip().upper()
                with engine.connect() as _conn:
                    rows = _conn.execute(sqlalchemy.text(
                        "SELECT name, commodity, stage, risk_score, assignee, created_at "
                        "FROM deals WHERE UPPER(name) LIKE :j OR UPPER(source_group) LIKE :j LIMIT 5"
                    ), {"j": f"%{job_code}%"}).fetchall()
                if rows:
                    lines = [f"🔍 **Resultados para `{job_code}`:**\n"]
                    for r in rows:
                        lines.append(
                            f"- **{r[0]}** | {r[1] or '?'} | Stage: {r[2] or '?'} "
                            f"| Risco: {r[3] or '?'} | Resp: {r[4] or '?'} | {str(r[5])[:10]}"
                        )
                    return "\n".join(lines)

            # ── Lista deals ativos ──────────────────────────────────
            if any(k in q_low for k in ("deals ativos", "pipeline ativo", "listar deals", "listar jobs")):
                with engine.connect() as _conn:
                    rows = _conn.execute(sqlalchemy.text(
                        "SELECT name, commodity, stage, COALESCE(source_group,'-'), created_at "
                        "FROM deals WHERE status='ativo' ORDER BY created_at DESC LIMIT 20"
                    )).fetchall()
                if rows:
                    lines = [f"📋 **{len(rows)} deals ativos:**\n"]
                    for r in rows:
                        lines.append(f"- **{r[0][:50]}** | {r[1] or '?'} | {r[2] or 'Lead'} | {r[3]} | {str(r[4])[:10]}")
                    return "\n".join(lines)
                return "Nenhum deal ativo no momento."

            # ── Action items / pendências ───────────────────────────
            if any(k in q_low for k in ("action item", "pendência", "pendencia", "tarefas abertas", "atribuição")):
                from models.database import MeetingActionItem, get_session as _gs
                _s = _gs()
                try:
                    _rows = (
                        _s.query(MeetingActionItem)
                        .filter(MeetingActionItem.status == "pendente")
                        .order_by(MeetingActionItem.priority, MeetingActionItem.created_at)
                        .limit(15)
                        .all()
                    )
                    if _rows:
                        _icon = {"critica": "🔴", "alta": "🟠", "media": "🟡", "baixa": "🟢"}
                        lines = [f"⚠️ **{len(_rows)} action items pendentes:**\n"]
                        for r in _rows:
                            ic = _icon.get(r.priority or "media", "•")
                            lines.append(f"{ic} **{r.responsible or '?'}** — {(r.action or '')[:120]}")
                        return "\n".join(lines)
                    return "Nenhum action item pendente no momento."
                finally:
                    _s.close()

        except Exception as _exc:
            logger.debug("_db_quick_lookup erro (nao critico): %s", _exc)

        return None  # escalate to Gemini

    # ── Render do historico ─────────────────────────────────────────
    for turn in st.session_state["samba_messages"]:
        role = turn.get("role")
        content = turn.get("content") or ""
        tool_calls = turn.get("tool_calls")

        if role == "user":
            with st.chat_message("user"):
                st.markdown(content)
        elif role == "assistant":
            with st.chat_message("assistant"):
                if tool_calls:
                    for tc in tool_calls:
                        st.caption(f"🔧 tool: `{tc.get('name')}` args={tc.get('args')}")
                if content:
                    st.markdown(content)
        elif role == "tool":
            if tool_calls:
                with st.expander(f"🛠️ resultado: {tool_calls[0].get('name')}", expanded=False):
                    st.json(tool_calls[0].get("result"))

    # ── Chat input ──────────────────────────────────────────────────
    # Quick Action inject: preenche o input se um botão foi pressionado
    if _qa_inject and "samba_messages" in st.session_state:
        st.session_state["_samba_prefill"] = _qa_inject

    user_input = st.chat_input("Pergunte ao Samba Assistant... ou use uma Ação Rápida acima")

    # Resolve prefill de Quick Actions
    if not user_input and st.session_state.get("_samba_prefill"):
        user_input = st.session_state.pop("_samba_prefill")

    if user_input:
        # 1) Persiste turno do usuario + render imediato.
        _append_turn(session_id=_sess, role="user", content=user_input)
        st.session_state["samba_messages"].append({
            "role": "user", "content": user_input, "tool_calls": None,
        })
        with st.chat_message("user"):
            st.markdown(user_input)

        # 2) DB-First: tenta responder sem LLM
        with st.chat_message("assistant"):
            placeholder = st.empty()

            db_reply = _db_quick_lookup(user_input)
            if db_reply:
                # Resposta instantânea do banco — sem LLM
                placeholder.markdown(db_reply)
                _append_turn(session_id=_sess, role="assistant", content=db_reply)
                st.session_state["samba_messages"].append({
                    "role": "assistant", "content": db_reply, "tool_calls": None,
                })
                st.caption("⚡ _Resposta direta do banco de dados (sem LLM)_")
            else:
                # 3) Escala para Gemini + RAG
                placeholder.markdown("_Consultando base de dados e manuais... aguarde._")
                try:
                    from services.gemini_api import chat_with_tools
                    from core.tool_registry import registry

                    gemini_history = _db_to_gemini(st.session_state["samba_messages"][:-1])
                    result = chat_with_tools(
                        user_message=user_input,
                        history=gemini_history,
                        tool_declarations=registry.to_gemini_declarations(),
                        tool_executor=lambda name, args: registry.execute(name, **args),
                    )
                    reply_text = result.get("text") or "(sem resposta)"
                    tool_trace = result.get("tool_calls") or []

                    if tool_trace:
                        for tc in tool_trace:
                            st.caption(f"🔧 tool: `{tc['name']}` args={tc['args']}")
                    placeholder.markdown(reply_text)

                    _persist_assistant(
                        session_id=_sess,
                        text=reply_text,
                        tool_calls_trace=tool_trace,
                    )
                    st.session_state["samba_messages"].append({
                        "role": "assistant",
                        "content": reply_text,
                        "tool_calls": [{"name": tc["name"], "args": tc["args"]} for tc in tool_trace] or None,
                    })
                    for tc in tool_trace:
                        st.session_state["samba_messages"].append({
                            "role": "tool",
                            "content": None,
                            "tool_calls": [{"name": tc["name"], "result": tc.get("result")}],
                        })

                except Exception as e:
                    placeholder.error(f"Erro no Assistant: {e}")

# ─────────────────────────────────────────────────────────────────
# ABA 10 — BASE DE CONHECIMENTO — helpers (nivel de modulo)
# IMPORTANTE: definidas FORA do with abas[9] para evitar que
# inspect.getsource (chamado por @st.cache_data) leia ate o EOF
# e falhe com "EOF in multi-line statement".
# ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def _kb_stats():
    """Carrega estatisticas da tabela corporate_knowledge."""
    from models.database import CorporateKnowledge, get_session
    s = get_session()
    try:
        rows = s.query(CorporateKnowledge).all()
        total_chunks  = len(rows)
        total_tokens  = sum(r.token_count or 0 for r in rows)
        n_with_embed  = sum(1 for r in rows if r.embedding and r.embedding != "null")
        from collections import defaultdict
        doc_agg = defaultdict(lambda: {"chunks": 0, "tokens": 0})
        for r in rows:
            doc_agg[r.document_name or "sem nome"]["chunks"] += 1
            doc_agg[r.document_name or "sem nome"]["tokens"] += r.token_count or 0
        return {
            "total_chunks":  total_chunks,
            "total_tokens":  total_tokens,
            "n_with_embed":  n_with_embed,
            "n_docs":        len(doc_agg),
            "doc_table":     sorted(doc_agg.items(), key=lambda x: x[1]["chunks"], reverse=True),
        }
    except Exception:
        return {"total_chunks": 0, "total_tokens": 0, "n_with_embed": 0, "n_docs": 0, "doc_table": []}
    finally:
        s.close()


@st.cache_data(ttl=15)
def _drive_sync_status():
    """Carrega estado do canal Drive Watch e page token."""
    from models.database import DriveSyncState, get_session
    from datetime import datetime, timezone
    try:
        s = get_session()
    except Exception:
        return {"channel_id": "N/A", "resource_id": "N/A", "page_token": "N/A",
                "exp_str": "N/A", "exp_color": "var(--samba-dim)", "days_left": None}
    try:
        def _get(key):
            try:
                row = s.query(DriveSyncState).filter_by(key=key).first()
                return row.value if row else None
            except Exception:
                return None

        channel_id   = _get("drive_channel_id")   or "N/A"
        resource_id  = _get("drive_resource_id")  or "N/A"
        page_token   = _get("changes_page_token") or "N/A"
        exp_ms_str   = _get("drive_channel_expiration_ms")
        exp_str      = "N/A"
        exp_color    = "var(--samba-dim)"
        days_left    = None

        if exp_ms_str and exp_ms_str.isdigit():
            exp_dt    = datetime.fromtimestamp(int(exp_ms_str) / 1000, tz=timezone.utc)
            days_left = (exp_dt - datetime.now(timezone.utc)).days
            exp_str   = exp_dt.strftime("%Y-%m-%d %H:%M UTC")
            exp_color = "#fa3232" if days_left <= 2 else ("#fa8200" if days_left <= 4 else "#329632")

        return {
            "channel_id":  channel_id,
            "resource_id": resource_id,
            "page_token":  page_token,
            "exp_str":     exp_str,
            "exp_color":   exp_color,
            "days_left":   days_left,
        }
    finally:
        s.close()


# ─────────────────────────────────────────────────────────────────
# ABA 10 — BASE DE CONHECIMENTO
# ─────────────────────────────────────────────────────────────────
with abas[9]:
    st.markdown('<div class="section-title">BASE DE CONHECIMENTO — CEREBRO VETORIAL RAG</div>', unsafe_allow_html=True)
    st.caption("Status em tempo real do CorporateKnowledge + sincronizacao Drive + busca semantica ao vivo.")

    # ── KPI row ──────────────────────────────────────────────────
    kb = _kb_stats()
    ds = _drive_sync_status()

    c1, c2, c3, c4, c5 = st.columns(5)
    kpi_style = (
        "background:var(--samba-bg-card);border:1px solid var(--samba-line);"
        "border-radius:8px;padding:16px 12px;text-align:center;"
    )
    with c1:
        st.markdown(
            f'<div style="{kpi_style}">'
            f'<div style="font-size:26px;font-weight:800;color:var(--samba-gold)">{kb["total_chunks"]}</div>'
            f'<div style="font-size:11px;color:var(--samba-dim);margin-top:4px">CHUNKS TOTAIS</div>'
            f'</div>', unsafe_allow_html=True
        )
    with c2:
        st.markdown(
            f'<div style="{kpi_style}">'
            f'<div style="font-size:26px;font-weight:800;color:var(--samba-ice)">{kb["n_docs"]}</div>'
            f'<div style="font-size:11px;color:var(--samba-dim);margin-top:4px">DOCUMENTOS</div>'
            f'</div>', unsafe_allow_html=True
        )
    with c3:
        pct = int(kb["n_with_embed"] / kb["total_chunks"] * 100) if kb["total_chunks"] else 0
        embed_color = "#329632" if pct == 100 else "#fa8200"
        st.markdown(
            f'<div style="{kpi_style}">'
            f'<div style="font-size:26px;font-weight:800;color:{embed_color}">{pct}%</div>'
            f'<div style="font-size:11px;color:var(--samba-dim);margin-top:4px">COM EMBEDDING</div>'
            f'</div>', unsafe_allow_html=True
        )
    with c4:
        st.markdown(
            f'<div style="{kpi_style}">'
            f'<div style="font-size:26px;font-weight:800;color:var(--samba-ice)">{kb["total_tokens"]:,}</div>'
            f'<div style="font-size:11px;color:var(--samba-dim);margin-top:4px">TOKENS TOTAIS</div>'
            f'</div>', unsafe_allow_html=True
        )
    with c5:
        dl_label = f"{ds['days_left']}d" if ds["days_left"] is not None else "—"
        st.markdown(
            f'<div style="{kpi_style}">'
            f'<div style="font-size:26px;font-weight:800;color:{ds["exp_color"]}">{dl_label}</div>'
            f'<div style="font-size:11px;color:var(--samba-dim);margin-top:4px">CANAL EXPIRA EM</div>'
            f'</div>', unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Secao: estado do Drive Watch + Force Sync ─────────────────
    col_drive, col_sync = st.columns([3, 1])

    with col_drive:
        st.markdown('<div class="section-title" style="font-size:11px">DRIVE WATCH — ESTADO DO CANAL</div>', unsafe_allow_html=True)
        canal_rows = [
            ("Channel ID",    ds["channel_id"][:52] + "…" if len(ds["channel_id"]) > 52 else ds["channel_id"]),
            ("Resource ID",   ds["resource_id"][:52] + "…" if len(ds["resource_id"]) > 52 else ds["resource_id"]),
            ("Page Token",    ds["page_token"][:30] + "…"  if len(ds["page_token"]) > 30  else ds["page_token"]),
            ("Expiracao",     ds["exp_str"]),
        ]
        rows_html = ""
        for label, val in canal_rows:
            rows_html += (
                f'<tr>'
                f'<td style="color:var(--samba-dim);font-size:11px;width:120px">{label}</td>'
                f'<td style="color:var(--samba-ice);font-family:monospace;font-size:11px">{val}</td>'
                f'</tr>'
            )
        exp_badge = (
            f'<span style="background:{ds["exp_color"]};color:#000;border-radius:4px;'
            f'padding:2px 8px;font-size:10px;font-weight:700">'
            f'{ds["days_left"]}d restantes</span>' if ds["days_left"] is not None else ""
        )
        st.markdown(
            f'<div class="samba-table-wrap">'
            f'<table class="samba-table"><tbody>{rows_html}</tbody></table>'
            f'</div>{exp_badge}',
            unsafe_allow_html=True,
        )

    with col_sync:
        st.markdown('<div class="section-title" style="font-size:11px">SINCRONIZACAO</div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("▶ Force Full Sync", key="_kb_force_sync", use_container_width=True):
            try:
                from tasks.agent_tasks import task_ingest_drive_files
                r = task_ingest_drive_files.delay(full_scan=True)
                st.success(f"Task despachada: {r.id[:16]}…")
            except Exception as e:
                st.error(f"Erro: {e}")
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔑 Renovar Canal", key="_kb_renew_channel", use_container_width=True):
            try:
                from tasks.agent_tasks import task_renew_drive_webhook
                r = task_renew_drive_webhook.delay(force=True)
                st.success(f"Task despachada: {r.id[:16]}…")
            except Exception as e:
                st.error(f"Erro: {e}")
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption("Modelo: paraphrase-multilingual-MiniLM-L12-v2")
        st.caption("Threshold RAG: score > 0.30")
        st.caption("Top-K injetado: 3 chunks")

    st.markdown("<br>", unsafe_allow_html=True)
    st.divider()

    # ── Tabela por documento ──────────────────────────────────────
    st.markdown('<div class="section-title" style="font-size:11px">CHUNKS POR DOCUMENTO</div>', unsafe_allow_html=True)

    if kb["doc_table"]:
        header_html = (
            '<tr>'
            '<th style="text-align:left">Documento</th>'
            '<th style="text-align:center">Chunks</th>'
            '<th style="text-align:right">Tokens</th>'
            '<th style="text-align:right">Tokens Medios</th>'
            '</tr>'
        )
        rows_html = ""
        for doc_name, agg in kb["doc_table"]:
            avg_tok = int(agg["tokens"] / agg["chunks"]) if agg["chunks"] else 0
            bar_pct = int(agg["chunks"] / kb["doc_table"][0][1]["chunks"] * 100)
            rows_html += (
                f'<tr>'
                f'<td style="color:var(--samba-ice);font-size:11px;max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{doc_name}">'
                f'<div style="display:flex;align-items:center;gap:8px">'
                f'<div style="width:{bar_pct}%;max-width:80px;height:3px;background:var(--samba-gold);border-radius:2px;flex-shrink:0"></div>'
                f'{doc_name[:55]}</div></td>'
                f'<td style="text-align:center;color:var(--samba-gold);font-weight:700">{agg["chunks"]}</td>'
                f'<td style="text-align:right;color:var(--samba-dim);font-family:monospace;font-size:11px">{agg["tokens"]:,}</td>'
                f'<td style="text-align:right;color:var(--samba-dim);font-family:monospace;font-size:11px">{avg_tok}</td>'
                f'</tr>'
            )
        st.markdown(
            f'<div class="samba-table-wrap" style="max-height:380px;overflow-y:auto">'
            f'<table class="samba-table"><thead>{header_html}</thead><tbody>{rows_html}</tbody></table>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("Nenhum chunk encontrado na base de conhecimento.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.divider()

    # ── Busca RAG ao vivo ─────────────────────────────────────────
    st.markdown('<div class="section-title" style="font-size:11px">BUSCA SEMANTICA — TESTE AO VIVO</div>', unsafe_allow_html=True)
    st.caption("Simula exatamente o que o Gemini recebe antes de cada chamada LLM.")

    col_q, col_k = st.columns([4, 1])
    with col_q:
        rag_query = st.text_input(
            "Query",
            placeholder="Ex: preco CIF soja Mexico porto Veracruz 50000 MT",
            label_visibility="collapsed",
            key="_kb_rag_query",
        )
    with col_k:
        rag_k = st.selectbox("Top-K", [3, 5, 10], index=0, key="_kb_rag_k", label_visibility="collapsed")

    if rag_query and st.button("Buscar", key="_kb_rag_search"):
        with st.spinner("Vetorizando query e buscando..."):
            try:
                import warnings; warnings.filterwarnings("ignore")
                from services.rag_search import buscar_contexto_corporativo
                resultado = buscar_contexto_corporativo(rag_query, limite_resultados=rag_k)
                if resultado:
                    st.markdown(
                        f'<div style="background:var(--samba-bg-card);border:1px solid var(--samba-line);'
                        f'border-left:3px solid var(--samba-gold);border-radius:8px;padding:16px;'
                        f'font-family:monospace;font-size:12px;color:var(--samba-ice);'
                        f'white-space:pre-wrap;max-height:500px;overflow-y:auto">'
                        f'{resultado[:3000]}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                    st.caption(f"Top-{rag_k} chunks retornados — threshold cosine > 0.30")
                else:
                    st.warning("Nenhum chunk com similaridade > 0.30 para esta query. Tente termos mais especificos.")
            except Exception as e:
                st.error(f"Erro na busca RAG: {e}")# ══════════════════════════════════════════════════════════════
