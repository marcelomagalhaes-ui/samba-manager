"""
dashboards/comercial_hub.py
============================
Módulo Comercial / Prospecção — Samba Export
Hub com 9 ambientes:
  1. Organização         — CRM & Deals (extração automática de leads via IA)
  2. Treinamento IA      — chatbot de capacitação da equipe
  3. Dúvidas da Equipe   — FAQ salva pela equipe
  4. Lousa               — whiteboard com IA
  5. Caixa dos Sócios    — inbox interno dos sócios
  6. Ref. Documentos     — biblioteca de documentos de exportação
  7. Anonimizador        — mascaramento de imagens localmente
  8. WhatsApp / Automações — automações e webhooks
  9. Prospecção          — pipeline de prospects
"""
from __future__ import annotations

import sys
import datetime
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ── Paleta Comercial — verde Samba ────────────────────────────────────────────
_COM_GREEN  = "#329632"
_COM_DARK   = "#1A2E1A"
_COM_LIGHT  = "#F0FDF4"
_COM_ORANGE = "#FA8200"

# ── Personas da equipe ────────────────────────────────────────────────────────
_USUARIOS = {
    "JA": "Jackeline",
    "PE": "Pedro",
    "CL": "Claudio",
    "ST": "Stella",
    "LB": "Leonardo",
    "MM": "Marcelo",
    "ND": "Nívio",
}

# ── CSS global do módulo — Padrão Enterprise ─────────────────────────────────
_CSS = """
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
/* ── Reset / layout ── */
.stApp { background: #f7f7f7 !important; font-family: 'Montserrat', sans-serif !important; }
[data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none !important; }
header, footer { display: none !important; }
.block-container { padding: 0 !important; max-width: 100% !important; }
.stMainBlockContainer { padding: 1.5rem 2rem 3rem !important; max-width: 1400px !important; margin: 0 auto !important; }

/* ── Header enterprise — branco + borda laranja ── */
.com-hdr {
    background: #ffffff;
    border-bottom: 4px solid #FA8200;
    padding: 14px 24px;
    display: flex; align-items: center; gap: 14px;
    min-height: 72px; box-sizing: border-box;
    box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    margin-bottom: 0;
    position: sticky; top: 0; z-index: 200;
}
.com-hdr-logo { font-size: 20px; font-weight: 900; color: #1a1a1a; }
.com-hdr-logo span { color: #FA8200; }
.com-hdr-tags { display: flex; gap: 8px; }
.com-htag { background: #FFF3E0; color: #FA8200; font-size: 10px; font-weight: 700;
    padding: 3px 10px; border-radius: 20px; letter-spacing: .5px; }
.com-htag.ai { background: #F3E5F5; color: #6A1B9A; }
.com-hdr-btn {
    width: 34px; height: 34px; border-radius: 7px;
    background: #f7f7f7;
    border: 1px solid #e8e8e8;
    display: inline-flex; align-items: center; justify-content: center;
    cursor: pointer; text-decoration: none; color: #777;
    transition: all .15s; flex-shrink: 0;
}
.com-hdr-btn:hover { border-color: #FA8200; color: #FA8200; background: #FFF3E0; }
.com-hdr-btn svg { width: 15px; height: 15px; }

/* ── Abas (tabs) ── */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px; background: transparent;
    border-bottom: 1px solid #e8e8e8;
    padding-bottom: 0;
}
.stTabs [data-baseweb="tab"] {
    font-family: Montserrat, sans-serif !important;
    font-size: 10px !important; font-weight: 700 !important;
    letter-spacing: 1px !important; text-transform: uppercase !important;
    color: #9CA3AF !important; background: transparent !important;
    border: none !important; padding: 12px 16px !important;
    border-radius: 0 !important;
    transition: color .15s !important;
}
.stTabs [data-baseweb="tab"]:hover { color: #FA8200 !important; }
.stTabs [aria-selected="true"] {
    color: #FA8200 !important;
    border-bottom: 3px solid #FA8200 !important;
    background: #FFF3E0 !important;
}

/* ── KPI cards ── */
.kpi-card {
    background: #fff; border: 1px solid #e8e8e8;
    border-radius: 10px; padding: 16px 18px;
    font-family: Montserrat, sans-serif;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
    transition: border-color .15s;
}
.kpi-card:hover { border-color: #FA8200; }
.kpi-card.highlight { border-left: 3px solid #FA8200; }
.kpi-label {
    font-size: 9px; font-weight: 700; letter-spacing: 2px;
    color: #9CA3AF; text-transform: uppercase; margin-bottom: 6px;
}
.kpi-value {
    font-size: 28px; font-weight: 900; color: #FA8200; line-height: 1;
}
.kpi-sub { font-size: 10px; color: #6B7280; margin-top: 4px; }

/* ── Section label ── */
.section-label {
    font-size: 9px; font-weight: 700; letter-spacing: 2px;
    color: #FA8200; text-transform: uppercase; margin-bottom: 10px;
    font-family: Montserrat, sans-serif;
}

/* ── Input area / wrap ── */
.input-wrap {
    background: #fff; border: 1px solid #E5E7EB; border-radius: 10px;
    padding: 20px 22px; margin-bottom: 18px;
    box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 1px 2px rgba(0,0,0,.04);
}

/* ── USER SELECTOR — pills laranja ── */
.user-selector-wrap { display: flex; gap: 10px; flex-wrap: wrap; margin-bottom: 16px; }
.user-pill {
    display: inline-flex; align-items: center; gap: 8px;
    padding: 8px 16px; border-radius: 30px;
    border: 2px solid #e8e8e8; background: #fff;
    font-size: 12px; font-weight: 700; font-family: Montserrat, sans-serif;
    cursor: pointer; transition: all .2s; color: #2d2d2d;
}
.user-pill:hover { border-color: #FA8200; }
.user-pill.active { border-color: #FA8200; background: #FA8200; color: #fff; }
.user-pill-avatar {
    width: 26px; height: 26px; border-radius: 50%;
    background: #FFF3E0; color: #FA8200;
    display: inline-flex; align-items: center; justify-content: center;
    font-weight: 900; font-size: 10px;
}
.user-pill.active .user-pill-avatar { background: rgba(255,255,255,.3); color: #fff; }

/* ── PASTE ZONE ── */
.paste-zone-wrap {
    background: #fff; border: 2px dashed #e8e8e8; border-radius: 12px;
    min-height: 200px; padding: 18px; transition: border-color .2s;
    position: relative;
}
.paste-zone-wrap:focus-within { border-color: #FA8200; box-shadow: 0 0 0 3px #FFF3E0; }
.paste-zone-label {
    font-size: 12px; font-weight: 700; color: #2d2d2d; margin-bottom: 10px;
    display: flex; align-items: center; gap: 8px;
}

/* ── AI OUTPUT — cabeçalho laranja ── */
.ai-out-wrap { background: #fff; border: 1px solid #e8e8e8; border-radius: 12px; overflow: hidden; }
.ai-out-header {
    background: #FA8200; color: #fff; padding: 12px 18px;
    font-weight: 800; font-size: 13px;
    display: flex; justify-content: space-between; align-items: center;
}
.ai-out-body { padding: 18px; }
.crm-fields-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.crm-field-item { background: #f7f7f7; border-radius: 8px; padding: 10px 14px; }
.crm-field-label { font-size: 10px; font-weight: 700; color: #777; letter-spacing: .5px; margin-bottom: 4px; text-transform: uppercase; }
.crm-field-value { font-size: 13px; font-weight: 600; color: #1a1a1a; }
.crm-field-value.empty { color: #ccc; font-style: italic; font-weight: 400; }

/* ── CRM Stats bar ── */
.crm-stats-bar { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
.crm-stat-item {
    background: #fff; border: 1px solid #e8e8e8; border-radius: 10px;
    padding: 12px 18px; flex: 1; min-width: 100px; text-align: center;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
}
.crm-stat-num { font-size: 22px; font-weight: 900; color: #FA8200; }
.crm-stat-lbl { font-size: 10px; color: #777; font-weight: 600; margin-top: 2px; }

/* ── CRM Cards ── */
.crm-card {
    background: #fff; border: 1px solid #e8e8e8;
    border-radius: 8px; padding: 16px 18px;
    margin-bottom: 10px; font-family: Montserrat, sans-serif;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
    transition: box-shadow .15s;
}
.crm-card:hover { box-shadow: 0 4px 14px rgba(0,0,0,.10); }
.crm-title { font-size: 13px; font-weight: 700; color: #1a1a1a; margin-bottom: 3px; }
.crm-sub   { font-size: 11px; color: #6B7280; font-weight: 500; margin-bottom: 10px; }
.crm-tags  { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 8px; }
.crm-tag   {
    font-size: 9px; font-weight: 700; letter-spacing: .8px;
    padding: 3px 9px; border-radius: 20px;
    background: #f7f7f7; color: #6B7280;
    text-transform: uppercase;
}
.crm-tag.green  { background: #E8F5E9; color: #2e7d32; }
.crm-tag.orange { background: #FFF3E0; color: #FA8200; }
.crm-tag.blue   { background: #E3F2FD; color: #1565C0; }
.crm-obs { font-size: 11px; color: #6B7280; line-height: 1.55; font-style: italic; }

/* ── Status badge ── */
.status-badge {
    display: inline-block; font-size: 9px; font-weight: 700;
    padding: 3px 10px; border-radius: 20px; letter-spacing: .5px;
}
.sb-novo        { background: #E8F5E9; color: #2e7d32; }
.sb-negociacao  { background: #FFF3E0; color: #FA8200; }
.sb-qualificado { background: #E3F2FD; color: #1565C0; }
.sb-perdido     { background: #FFEBEE; color: #c62828; }
.sb-aguardando  { background: #f7f7f7; color: #6B7280; }

/* ── Empty state ── */
.empty-state {
    text-align: center; padding: 48px 24px;
    font-family: Montserrat, sans-serif; color: #9CA3AF;
    background: #fff; border: 1px solid #E5E7EB;
    border-radius: 10px; margin-top: 8px;
}
.empty-state-title { font-size: 13px; font-weight: 700; margin-bottom: 6px; color: #6B7280; }
.empty-state-sub   { font-size: 11px; color: #9CA3AF; }

/* ── Em breve (placeholder) ── */
.em-breve-wrap {
    background: #fff; border: 1px dashed #D1D5DB; border-radius: 12px;
    padding: 48px 24px; text-align: center; font-family: Montserrat, sans-serif;
    margin-top: 16px;
}
.em-breve-label {
    font-size: 9px; font-weight: 700; letter-spacing: 2px; color: #9CA3AF;
    text-transform: uppercase; margin-bottom: 12px;
}
.em-breve-title {
    font-size: 15px; font-weight: 800; color: #111827; margin-bottom: 8px;
}
.em-breve-desc {
    font-size: 11px; color: #6B7280; line-height: 1.6; max-width: 460px; margin: 0 auto;
}
.feature-pill {
    font-size: 10px; padding: 5px 14px; border-radius: 20px;
    font-weight: 700; font-family: Montserrat, sans-serif;
    display: inline-block; margin: 4px;
}

/* ── Chat Treinamento ── */
.chat-bubble-user {
    background: #FFF3E0; border-radius: 12px 12px 3px 12px;
    padding: 10px 14px; margin: 6px 0 6px 40px;
    font-size: 12px; color: #7B3800; font-family: Montserrat, sans-serif;
    line-height: 1.55;
}
.chat-bubble-ai {
    background: #fff; border: 1px solid #e8e8e8;
    border-radius: 12px 12px 12px 3px;
    padding: 10px 14px; margin: 6px 40px 6px 0;
    font-size: 12px; color: #1a1a1a; font-family: Montserrat, sans-serif;
    line-height: 1.55;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
}
.chat-sender {
    font-size: 9px; font-weight: 700; letter-spacing: 1.5px; color: #9CA3AF;
    text-transform: uppercase; margin-bottom: 3px; font-family: Montserrat, sans-serif;
}

/* ── User card sidebar (Treinamento) ── */
.user-card-sidebar {
    background: #fff; border: 2px solid #e8e8e8; border-radius: 12px;
    padding: 14px 16px; cursor: pointer; transition: all .2s;
    display: flex; align-items: center; gap: 12px; margin-bottom: 8px;
    font-family: Montserrat, sans-serif;
}
.user-card-sidebar:hover { border-color: #FA8200; }
.user-card-sidebar.active { border-color: #FA8200; background: #FFF3E0; }
.uca { width: 40px; height: 40px; border-radius: 50%; display: flex; align-items: center;
    justify-content: center; font-weight: 900; font-size: 14px; flex-shrink: 0; }
.user-card-name { font-weight: 800; font-size: 13px; color: #1a1a1a; }
.user-card-role { font-size: 10px; color: #777; margin-top: 2px; }

/* ── Persona selector (avatar buttons) ── */
.persona-btn {
    display: inline-flex; align-items: center; justify-content: center;
    width: 36px; height: 36px; border-radius: 50%;
    border: 2px solid #e8e8e8; font-size: 11px; font-weight: 800;
    background: #f7f7f7; color: #6B7280; cursor: pointer;
    transition: all .15s; font-family: Montserrat, sans-serif;
    text-decoration: none;
}
.persona-btn.active { border-color: #FA8200; background: #FFF3E0; color: #FA8200; }
.persona-btn:hover { border-color: #FA8200; }

/* ── FAQ Tags ── */
.faq-tag {
    display: inline-block; font-size: 9px; font-weight: 700; letter-spacing: .8px;
    padding: 3px 10px; border-radius: 20px; cursor: pointer;
    background: #f7f7f7; color: #6B7280;
    text-transform: uppercase; margin: 2px; transition: all .15s;
}
.faq-tag:hover, .faq-tag.active { background: #FFF3E0; color: #FA8200; }

/* ── Kanban (Caixa dos Sócios) ── */
.kanban-header { padding: 8px 12px; border-radius: 8px 8px 0 0; font-weight: 800;
    font-size: 11px; display: flex; justify-content: space-between; align-items: center; }
.kanban-body { min-height: 80px; background: #f7f7f7; border: 1px solid #e8e8e8;
    border-top: none; border-radius: 0 0 8px 8px; padding: 8px;
    display: flex; flex-direction: column; gap: 6px; max-height: 400px; overflow-y: auto; }
.kanban-card { background: #fff; border: 1px solid #e8e8e8; border-radius: 8px; padding: 10px 12px; }
.kanban-card.p-urgente { border-left: 3px solid #c62828; }
.kanban-card.p-media   { border-left: 3px solid #FA8200; }
.kanban-card.p-normal  { border-left: 3px solid #2e7d32; }
.kanban-empty { text-align: center; padding: 20px 8px; color: #bbb; font-size: 11px; }

/* ── Documento Ref cards ── */
.doc-ref-card {
    background: #fff; border: 1px solid #E5E7EB; border-radius: 10px;
    padding: 16px 18px; margin-bottom: 10px; font-family: Montserrat, sans-serif;
    box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 1px 2px rgba(0,0,0,.04);
    transition: box-shadow .15s;
}
.doc-ref-card:hover { box-shadow: 0 4px 14px rgba(0,0,0,.10); }
.doc-ref-name { font-size: 14px; font-weight: 700; color: #111827; margin-bottom: 4px; }
.doc-ref-desc { font-size: 12px; color: #6B7280; line-height: 1.55; }
.doc-cat-badge {
    display: inline-block; font-size: 9px; font-weight: 700; letter-spacing: .8px;
    padding: 3px 10px; border-radius: 20px; text-transform: uppercase; margin-bottom: 8px;
}
/* Badge categorias — padrão enterprise */
.cat-qualidade   { background: #EFF6FF; color: #3B82F6; }
.cat-exportacao  { background: #F0FDF4; color: #329632; }
.cat-logistica   { background: #FFF7ED; color: #EA580C; }
.cat-financeiro  { background: #FEFCE8; color: #CA8A04; }
.cat-compliance  { background: #F0FDF4; color: #329632; }
.cat-juridico    { background: #FDF4FF; color: #9333EA; }
.cat-governo     { background: #F0F9FF; color: #0284C7; }
.cat-inspecao    { background: #FFF1F2; color: #E11D48; }

/* Filtro por categoria — pills clicáveis */
.cat-filter-pill {
    display: inline-block; font-size: 9px; font-weight: 700; letter-spacing: .8px;
    padding: 4px 12px; border-radius: 20px; cursor: pointer;
    background: #F9FAFB; color: #6B7280;
    text-transform: uppercase; margin: 3px; transition: all .15s;
}
.cat-filter-pill.active {
    background: #FFF3E0; color: #FA8200;
}
.cat-filter-pill:hover { color: #FA8200; }

/* ── Caixa dos Sócios ── */
.inbox-card {
    background: #fff; border: 1px solid #E5E7EB; border-radius: 8px;
    padding: 14px 16px; margin-bottom: 8px; font-family: Montserrat, sans-serif;
    box-shadow: 0 1px 3px rgba(0,0,0,.06), 0 1px 2px rgba(0,0,0,.04);
}
.inbox-card.prio-urgente { border-left: 3px solid #DC2626; }
.inbox-card.prio-media   { border-left: 3px solid #EA580C; }
.inbox-card.prio-normal  { border-left: 3px solid #329632; }
.inbox-meta { font-size: 10px; color: #9CA3AF; margin-top: 6px; }

/* Badge prioridade — pills sem emojis */
.prio-badge {
    display: inline-block; font-size: 9px; font-weight: 700;
    padding: 3px 10px; border-radius: 20px; letter-spacing: .5px;
}
.pb-urgente { background: #FEF2F2; color: #DC2626; }
.pb-media   { background: #FFF7ED; color: #EA580C; }
.pb-normal  { background: #F0FDF4; color: #329632; }

/* ── Anonimizador upload zone ── */
.upload-zone {
    border: 2px dashed #D1D5DB; border-radius: 10px;
    padding: 36px 24px; text-align: center;
    font-family: Montserrat, sans-serif; color: #9CA3AF;
    background: #FAFAFA;
}
.upload-zone-icon { margin-bottom: 10px; }
.upload-zone-title { font-size: 13px; font-weight: 700; color: #6B7280; margin-bottom: 4px; }
.upload-zone-sub   { font-size: 11px; color: #9CA3AF; }

/* ── Botões ocultos sob cards (user selector) ── */
[class*="st-key-crm_usr_"] button,
[class*="st-key-train_usr_"] button {
    opacity: 0 !important;
    position: absolute !important;
    inset: 0 !important;
    width: 100% !important;
    height: 100% !important;
    cursor: pointer !important;
    z-index: 10 !important;
    border: none !important;
    background: transparent !important;
}
[class*="st-key-crm_usr_"],
[class*="st-key-train_usr_"] {
    position: relative !important;
    margin-top: -44px !important;
    height: 44px !important;
    overflow: visible !important;
}

/* ── Notificação local (info banner) ── */
.info-banner {
    background: #FEFCE8; border: 1px solid #FDE047;
    border-radius: 8px; padding: 10px 14px;
    font-family: Montserrat, sans-serif; font-size: 11px; color: #713F12;
    margin-bottom: 16px;
}

/* ── Streamlit overrides ── */
div[data-testid="stTextInput"] input:focus,
div[data-testid="stTextArea"] textarea:focus {
    border-color: #FA8200 !important;
    box-shadow: 0 0 0 1px #FA8200 !important;
}
div[data-testid="stButton"] button[kind="primary"] {
    background: #FA8200 !important;
    border-color: #FA8200 !important;
    color: #fff !important;
    font-family: Montserrat, sans-serif !important;
    font-weight: 700 !important;
    font-size: 12px !important;
    letter-spacing: .5px !important;
    border-radius: 8px !important;
    transition: background .15s !important;
}
div[data-testid="stButton"] button[kind="primary"]:hover {
    background: #e07300 !important;
    border-color: #e07300 !important;
}
div[data-testid="stButton"] button[kind="secondary"] {
    background: #fff !important;
    border: 1px solid #e8e8e8 !important;
    color: #6B7280 !important;
    font-family: Montserrat, sans-serif !important;
    font-weight: 700 !important;
    font-size: 12px !important;
    border-radius: 8px !important;
    transition: border-color .15s !important;
}
div[data-testid="stButton"] button[kind="secondary"]:hover {
    border-color: #FA8200 !important;
    color: #FA8200 !important;
}

/* ── Botão Portal (home) — fixed sobre o header ── */
.st-key-com_portal_btn,
[data-testid="element-container"].st-key-com_portal_btn,
div.st-key-com_portal_btn {
    position: fixed !important;
    top: 18px !important;
    right: 110px !important;
    z-index: 99999 !important;
    width: 34px !important;
    height: 34px !important;
}
.st-key-com_portal_btn > div,
.st-key-com_portal_btn [data-testid="stButton"] {
    width: 34px !important;
    height: 34px !important;
}
.st-key-com_portal_btn button,
.st-key-com_portal_btn [data-testid="stButton"] button {
    width: 34px !important;
    height: 34px !important;
    min-height: 34px !important;
    padding: 0 !important;
    background: #f7f7f7 !important;
    border: 1px solid #e8e8e8 !important;
    border-radius: 7px !important;
    color: #777 !important;
    font-size: 16px !important;
    line-height: 1 !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
}
.st-key-com_portal_btn button:hover,
.st-key-com_portal_btn [data-testid="stButton"] button:hover {
    border-color: #FA8200 !important;
    color: #FA8200 !important;
    background: #FFF3E0 !important;
}
</style>
"""

# ── Status badge helper ───────────────────────────────────────────────────────
def _status_badge(status: str) -> str:
    cls = {
        "Novo":          "sb-novo",
        "Em Negociação": "sb-negociacao",
        "Qualificado":   "sb-qualificado",
        "Perdido":       "sb-perdido",
        "Aguardando":    "sb-aguardando",
    }.get(status, "sb-aguardando")
    return f'<span class="status-badge {cls}">{status or "Novo"}</span>'


def _confianca_tag(c: str) -> str:
    cls = {"Alto": "green", "Médio": "orange", "Baixo": ""}.get(c, "")
    return f'<span class="crm-tag {cls}">{c}</span>' if c else ""


# ── Tab 1 — Organização (CRM & Deals) ────────────────────────────────────────
def _tab_crm():
    try:
        from agents.crm_agent import create_crm_table, extract_crm_multi, save_crm_record, load_crm_leads
        create_crm_table()
    except Exception as e:
        st.error(f"Erro ao inicializar CRM: {e}")
        return

    st.markdown("""
<div style="font-size:22px;font-weight:900;color:#1a1a1a;margin-bottom:4px;font-family:Montserrat,sans-serif">
📋 <span style="color:#FA8200">Organização</span> — Cole qualquer coisa aqui</div>
<div style="color:#777;font-size:12px;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #e8e8e8;font-family:Montserrat,sans-serif">
Cole texto, mensagens do WhatsApp, prints de conversa, e-mails. A IA organiza tudo no formato CRM da Samba Export.</div>
""", unsafe_allow_html=True)

    # ── Seletor de usuário (quem está colando) ────────────────────────────────
    st.markdown("""<div style="font-weight:700;font-size:12px;color:#FA8200;margin-bottom:10px;font-family:Montserrat,sans-serif">👤 Quem está colando?</div>""", unsafe_allow_html=True)

    _user_colors = {
        "JA": ("#FCE4EC","#E91E63"), "PE": ("#E3F2FD","#1565C0"),
        "CL": ("#E8F5E9","#2e7d32"), "ST": ("#F3E5F5","#6A1B9A"),
        "LB": ("#FBE9E7","#E65100"), "MM": ("#EDE7F6","#4527A0"),
        "ND": ("#E0F2F1","#00695C"),
    }
    if "crm_user" not in st.session_state:
        st.session_state["crm_user"] = "JA"

    pill_cols = st.columns(len(_USUARIOS))
    for i, (code, name) in enumerate(_USUARIOS.items()):
        with pill_cols[i]:
            is_active = st.session_state.get("crm_user") == code
            bg_c, txt_c = _user_colors.get(code, ("#f7f7f7","#777"))
            active_style = f"border-color:#FA8200;background:#FA8200;color:#fff;" if is_active else f"border-color:#e8e8e8;background:#fff;color:#2d2d2d;"
            avatar_style = f"background:rgba(255,255,255,.3);color:#fff;" if is_active else f"background:{bg_c};color:{txt_c};"
            st.markdown(f"""
<div style="display:flex;justify-content:center">
  <span style="display:inline-flex;align-items:center;gap:6px;padding:7px 12px;border-radius:30px;
    border:2px solid;{active_style}font-size:11px;font-weight:700;font-family:Montserrat,sans-serif;
    cursor:pointer;white-space:nowrap">
    <span style="width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;
      justify-content:center;font-weight:900;font-size:9px;{avatar_style}">{code}</span>
    {name.split()[0]}
  </span>
</div>""", unsafe_allow_html=True)
            if st.button(f"▸ {name.split()[0]}", key=f"crm_usr_{code}",
                         help=f"Selecionar {name}", use_container_width=True):
                st.session_state["crm_user"] = code
                st.rerun()

    current_user = _USUARIOS.get(st.session_state.get("crm_user","JA"), "Usuário")

    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

    # ── Layout 2 colunas: paste zone + output ────────────────────────────────
    left_col, right_col = st.columns([1, 1], gap="medium")

    with left_col:
        # Zona de paste
        st.markdown(f"""
<div style="background:#fff;border:1px solid #e8e8e8;border-radius:12px;overflow:hidden;margin-bottom:14px">
  <div style="padding:12px 16px;background:#f7f7f7;border-bottom:1px solid #e8e8e8;
    display:flex;justify-content:space-between;align-items:center">
    <div style="font-weight:700;font-size:12px;font-family:Montserrat,sans-serif">
      📥 Cole aqui — mensagem de <span style="color:#FA8200">{current_user}</span>
    </div>
  </div>""", unsafe_allow_html=True)
        raw_msg = st.text_area(
            "Cole a mensagem",
            height=160,
            key="crm_input_text",
            placeholder=(
                "Cole aqui qualquer coisa:\n\n"
                "• Mensagem do LinkedIn ou WhatsApp\n"
                "• E-mail de um comprador\n"
                "• Texto com nome, empresa, produto pedido\n"
                "• Qualquer informação sobre um lead\n\n"
                "A IA vai entender e organizar tudo automaticamente."
            ),
            label_visibility="collapsed",
        )
        st.markdown("</div>", unsafe_allow_html=True)

        # Ações
        act_c1, act_c2, act_c3 = st.columns([2, 1, 1])
        with act_c1:
            fonte = st.selectbox("Origem", ["WhatsApp", "E-mail", "LinkedIn", "Outro"], key="crm_fonte")
        with act_c2:
            extrair = st.button("⚡ Organizar com IA", key="crm_extrair_btn",
                                use_container_width=True, type="primary")
        with act_c3:
            if st.button("🗑️ Limpar", key="crm_limpar_btn", use_container_width=True):
                if "_crm_previews" in st.session_state:
                    del st.session_state["_crm_previews"]
                st.rerun()

        # Exemplos rápidos
        st.markdown("""
<div style="background:#FFFDE7;border:1px solid #FDE047;border-radius:10px;padding:14px 16px;margin-top:6px">
  <div style="font-weight:700;font-size:12px;color:#7B5800;margin-bottom:8px;font-family:Montserrat,sans-serif">💡 Exemplos do que você pode colar</div>""", unsafe_allow_html=True)
        _exemplos = [
            ("linkedin", "Mensagem do LinkedIn"),
            ("whatsapp", "Conversa de WhatsApp"),
            ("email", "E-mail de cotação"),
            ("notas", "Anotações sobre contato"),
        ]
        for key, label in _exemplos:
            _ex_texts = {
                "linkedin": "Hello, I'm Rahul from GlobalAgro India. We're looking for 5,000 MT of Sugar ICUMSA 45 CIF Mumbai. Can you share your best offer?",
                "whatsapp": "Oi Marcelo, sou João da ABC Foods Egito. Tenho interesse em 10.000 MT de soja GMO CIF Alexandria p/ Outubro/2026. Podem enviar cotação?",
                "email": "Dear Team, We represent a buyer in Saudi Arabia interested in 2,000 MT of Chicken Feet per month, CIF Jeddah. Please send your best CIF price.",
                "notas": "Tejinder Singh - IndiaGrains Ltd Mumbai - Quer açúcar ICUMSA 45 - 3000 MT/mês - CIF Mundra - Já importa commodities",
            }
            if st.button(f"▶ {label}", key=f"ex_{key}", use_container_width=True):
                st.session_state["crm_input_text"] = _ex_texts[key]
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    with right_col:
        # Painel de output — cabeçalho laranja
        previews_out = st.session_state.get("_crm_out_display", {})
        if previews_out:
            st.markdown(f"""
<div class="ai-out-wrap">
  <div class="ai-out-header">
    ✨ Resultado Organizado pela IA
    <span style="font-size:11px;font-weight:400;opacity:.9">por {_USUARIOS.get(previews_out.get('user','JA'),'?')}</span>
  </div>
  <div class="ai-out-body">
    <div class="crm-fields-grid">""", unsafe_allow_html=True)
            _fields = [
                ("Nome", previews_out.get("nome","")), ("Empresa", previews_out.get("empresa","")),
                ("Commodity", previews_out.get("commodity","")), ("Volume", previews_out.get("volume","")),
                ("Incoterm", previews_out.get("incoterm","")), ("País Destino", previews_out.get("pais_destino","")),
                ("Porto Destino", previews_out.get("porto_destino","")), ("Status", previews_out.get("status_lead","Novo")),
            ]
            for lbl, val in _fields:
                val_cls = "crm-field-value" if val else "crm-field-value empty"
                val_txt = val if val else "—"
                st.markdown(f"""
<div class="crm-field-item">
  <div class="crm-field-label">{lbl}</div>
  <div class="{val_cls}">{val_txt}</div>
</div>""", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
            obs = previews_out.get("observacoes","")
            if obs:
                st.markdown(f"""
<div style="margin-top:12px;background:#f7f7f7;border-radius:8px;padding:10px 14px;
  font-family:Montserrat,sans-serif;font-size:12px;color:#2d2d2d;line-height:1.6">
<span style="font-size:9px;font-weight:700;color:#777;text-transform:uppercase;letter-spacing:.5px">Observações</span><br>
{obs}</div>""", unsafe_allow_html=True)
            st.markdown("</div></div>", unsafe_allow_html=True)
        else:
            st.markdown("""
<div class="ai-out-wrap">
  <div class="ai-out-header">✨ Resultado Organizado pela IA</div>
  <div class="ai-out-body" style="text-align:center;padding:40px 20px;color:#777">
    <div style="font-size:48px;margin-bottom:16px">📋</div>
    <div style="font-size:14px;font-weight:700;color:#2d2d2d;margin-bottom:8px;font-family:Montserrat,sans-serif">
      Cole algo à esquerda e clique em "Organizar com IA"</div>
    <div style="font-size:12px;line-height:1.7;font-family:Montserrat,sans-serif">
      A IA vai extrair automaticamente:<br>
      Nome · Empresa · Commodity · Volume · Porto · País · Status</div>
  </div>
</div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    if extrair:
        if not (raw_msg or "").strip():
            st.warning("Cole uma mensagem antes de extrair.")
        else:
            with st.spinner("⚡ Extraindo campos CRM via IA…"):
                records = extract_crm_multi(raw_msg.strip(), fonte=fonte)

            first_err = next((r["error"] for r in records if r.get("error")), None)
            if first_err:
                st.error(f"Erro: {first_err}")
            else:
                st.session_state["_crm_previews"] = records
                # Store first record for display panel
                if records:
                    disp = dict(records[0])
                    disp["user"] = st.session_state.get("crm_user","JA")
                    st.session_state["_crm_out_display"] = disp
                n = len(records)
                msg_ok = (f"✅ {n} lead{'s' if n > 1 else ''} extraído{'s' if n > 1 else ''}! "
                          f"Revise abaixo e confirme para salvar.")
                st.success(msg_ok)

    # ── Preview multi-lead ─────────────────────────────────────────────────────
    previews = st.session_state.get("_crm_previews", [])
    if previews:
        st.markdown("---")
        n_prev = len(previews)
        st.markdown(
            f'<div class="section-label">Revisão antes de salvar '
            f'— {n_prev} lead{"s" if n_prev > 1 else ""} identificado{"s" if n_prev > 1 else ""}</div>',
            unsafe_allow_html=True,
        )

        for idx, r in enumerate(previews):
            if n_prev > 1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:700;color:#FA8200;"
                    f"font-family:Montserrat,sans-serif;margin:12px 0 6px'>"
                    f"LEAD {idx+1} — {r.get('commodity','?').upper()}</div>",
                    unsafe_allow_html=True,
                )

            sfx = str(idx)
            ec1, ec2, ec3 = st.columns(3)
            with ec1:
                r["nome"]    = st.text_input("Nome do contato", value=r.get("nome",""),    key=f"crm_e_nome_{sfx}")
                r["empresa"] = st.text_input("Empresa",         value=r.get("empresa",""), key=f"crm_e_empresa_{sfx}")
                r["fonte"]   = st.selectbox("Origem",["WhatsApp","E-mail","LinkedIn","Outro"],
                                             index=["WhatsApp","E-mail","LinkedIn","Outro"].index(r.get("fonte","Outro")),
                                             key=f"crm_e_fonte_{sfx}")
            with ec2:
                r["commodity"] = st.text_input("Commodity",   value=r.get("commodity",""), key=f"crm_e_comm_{sfx}")
                r["volume"]    = st.text_input("Volume (MT)", value=r.get("volume",""),    key=f"crm_e_vol_{sfx}")
                r["incoterm"]  = st.text_input("Incoterm",    value=r.get("incoterm",""),  key=f"crm_e_inco_{sfx}")
            with ec3:
                r["pais_destino"]  = st.text_input("País destino",  value=r.get("pais_destino",""),  key=f"crm_e_pais_{sfx}")
                r["porto_destino"] = st.text_input("Porto destino", value=r.get("porto_destino",""), key=f"crm_e_porto_{sfx}")
                _status_opts = ["Novo","Em Negociação","Qualificado","Aguardando","Perdido"]
                _status_val  = r.get("status_lead","Novo") if r.get("status_lead","Novo") in _status_opts else "Novo"
                r["status_lead"] = st.selectbox("Status", _status_opts,
                                                index=_status_opts.index(_status_val),
                                                key=f"crm_e_status_{sfx}")
            r["observacoes"] = st.text_area("Observações", value=r.get("observacoes",""),
                                            height=68, key=f"crm_e_obs_{sfx}")

        bc1, bc2, _ = st.columns([1, 1, 3])
        with bc1:
            lbl_salvar = f"Salvar {n_prev} Lead{'s' if n_prev > 1 else ''}"
            if st.button(lbl_salvar, key="crm_salvar", type="primary", use_container_width=True):
                from agents.lead_pipeline import run_pipeline
                pipeline_results = []
                for r in previews:
                    res = run_pipeline(r, sync_sheets=False)
                    pipeline_results.append({"record": r, "pipeline": res})
                if pipeline_results:
                    st.session_state["_crm_pipeline_results"] = pipeline_results
                    del st.session_state["_crm_previews"]
                    st.rerun()
                else:
                    st.error("Erro ao processar leads.")
        with bc2:
            if st.button("Descartar", key="crm_descartar", use_container_width=True):
                if "_crm_previews" in st.session_state:
                    del st.session_state["_crm_previews"]
                st.rerun()

    # ── Painel pós-salvamento: dedup + sheets + próximos passos ───────────────
    pipeline_results = st.session_state.get("_crm_pipeline_results", [])
    if pipeline_results:
        st.markdown("---")
        for pr_idx, pr in enumerate(pipeline_results):
            rec  = pr["record"]
            pipe = pr["pipeline"]
            db   = pipe.get("db", {})
            dup  = pipe.get("duplicate", {})
            nxt  = pipe.get("next_steps", [])
            rid  = pipe.get("record_id")
            comm = (rec.get("commodity") or "Lead").upper()

            # ── Cabeçalho do resultado
            if len(pipeline_results) > 1:
                st.markdown(
                    f"<div style='font-size:11px;font-weight:700;color:#FA8200;"
                    f"font-family:Montserrat,sans-serif;margin:14px 0 6px'>"
                    f"RESULTADO — {comm}</div>",
                    unsafe_allow_html=True,
                )

            # ── Status do banco
            if db.get("ok"):
                st.success(f"✅ {db['detail']}")
            else:
                st.error(f"❌ {db.get('detail','Erro desconhecido')}")
                continue

            # ── Deduplicação
            if dup.get("is_duplicate"):
                st.warning(f"⚠️ {dup['detail']}")
            else:
                st.info(f"🆕 {dup['detail']}")

            # ── Pergunta: incluir na planilha?
            sheets_key = f"_crm_sheets_done_{pr_idx}_{rid}"
            if not st.session_state.get(sheets_key):
                st.markdown(
                    "<div style='background:#F0FDF4;border:1px solid #86EFAC;"
                    "border-radius:8px;padding:12px 16px;margin:10px 0;"
                    "font-family:Montserrat,sans-serif;font-size:12px;color:#166534'>"
                    "📊 <b>Deseja incluir este lead na planilha de controle</b> "
                    "(aba <i>todos andamento</i>)?<br>"
                    "<span style='font-size:10px;color:#4B7A57'>O SpreadsheetSyncAgent "
                    "irá gerar a Ficha PDF e arquivar no Drive automaticamente.</span>"
                    "</div>",
                    unsafe_allow_html=True,
                )
                s1, s2, _ = st.columns([1, 1, 4])
                with s1:
                    if st.button("✅ Sim, incluir", key=f"crm_sheets_sim_{pr_idx}",
                                 type="primary", use_container_width=True):
                        from agents.lead_pipeline import sync_to_sheets
                        with st.spinner("Sincronizando com Sheets…"):
                            sh = sync_to_sheets(rec, record_id=rid)
                        if sh["ok"]:
                            st.success(f"📊 {sh['detail']}")
                            st.markdown(
                                f'<a href="{sh["sheet_url"]}" target="_blank" '
                                f'style="color:#FA8200;font-size:11px;font-weight:700">↗ Abrir planilha</a>',
                                unsafe_allow_html=True,
                            )
                        else:
                            st.error(f"❌ {sh['detail']}")
                        st.session_state[sheets_key] = True
                        st.rerun()
                with s2:
                    if st.button("⏭ Não agora", key=f"crm_sheets_nao_{pr_idx}",
                                 use_container_width=True):
                        st.session_state[sheets_key] = True
                        st.rerun()
            else:
                st.markdown(
                    "<div style='font-size:10px;color:#6B7280;font-family:Montserrat,sans-serif;"
                    "margin:4px 0'>📊 Planilha: decisão registrada</div>",
                    unsafe_allow_html=True,
                )

            # ── Próximos passos
            if nxt:
                st.markdown(
                    "<div style='font-size:9px;font-weight:700;letter-spacing:2px;"
                    "color:#FA8200;font-family:Montserrat,sans-serif;"
                    "margin:14px 0 8px;text-transform:uppercase'>Próximos Passos Recomendados</div>",
                    unsafe_allow_html=True,
                )
                for step in nxt:
                    priority_color = "#DC2626" if step["priority"] == 1 else "#EA580C" if step["priority"] == 2 else "#2e7d32"
                    st.markdown(
                        f"<div style='background:#fff;border:1px solid #E5E7EB;"
                        f"border-left:3px solid {priority_color};"
                        f"border-radius:6px;padding:10px 14px;margin-bottom:6px;"
                        f"font-family:Montserrat,sans-serif'>"
                        f"<div style='font-size:12px;font-weight:700;color:#111827'>{step['label']}</div>"
                        f"<div style='font-size:10px;color:#6B7280;margin-top:3px'>{step['reason']}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

                # ── Botões de ação
                n_btns = sum(1 for s in nxt if s["type"] in ("loi", "icpo", "cotacao"))
                if n_btns:
                    btn_cols = st.columns(min(n_btns + 1, 4))
                    btn_idx  = 0
                    for step in nxt:
                        if step["type"] == "loi":
                            with btn_cols[btn_idx]:
                                if st.button("📄 Gerar LOI",
                                             key=f"crm_goto_loi_{pr_idx}",
                                             use_container_width=True):
                                    # Pré-carrega empresa/commodity e vai para aba LOI
                                    st.session_state["loi_prefill_empresa"]   = rec.get("empresa","")
                                    st.session_state["loi_prefill_commodity"] = rec.get("commodity","")
                                    st.session_state["loi_prefill_pais"]      = rec.get("pais_destino","")
                                    st.session_state["current_view"] = "documentos"
                                    st.rerun()
                            btn_idx += 1
                        elif step["type"] == "cotacao":
                            with btn_cols[btn_idx]:
                                if st.button("📊 Ir para Cotação",
                                             key=f"crm_goto_cot_{pr_idx}",
                                             use_container_width=True):
                                    st.session_state["current_view"] = "cotacao"
                                    st.rerun()
                            btn_idx += 1

        # ── Botão fechar painel
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        if st.button("✖ Fechar painel", key="crm_close_pipeline",
                     use_container_width=False):
            del st.session_state["_crm_pipeline_results"]
            # Limpa flags de sheets
            keys_to_del = [k for k in st.session_state if k.startswith("_crm_sheets_done_")]
            for k in keys_to_del:
                del st.session_state[k]
            st.rerun()

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
    st.markdown('<div class="section-label">📊 Leads cadastrados</div>', unsafe_allow_html=True)

    # ── Stats bar ─────────────────────────────────────────────────────────────
    all_leads_stats = load_crm_leads(limit=500)
    _s_total  = len(all_leads_stats)
    _s_novo   = sum(1 for l in all_leads_stats if l.get("status_lead") == "Novo")
    _s_neg    = sum(1 for l in all_leads_stats if l.get("status_lead") == "Em Negociação")
    _s_qual   = sum(1 for l in all_leads_stats if l.get("status_lead") == "Qualificado")
    _s_perd   = sum(1 for l in all_leads_stats if l.get("status_lead") == "Perdido")
    st.markdown(f"""
<div class="crm-stats-bar">
  <div class="crm-stat-item"><div class="crm-stat-num">{_s_total}</div><div class="crm-stat-lbl">Total</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_s_novo}</div><div class="crm-stat-lbl">Novos</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_s_neg}</div><div class="crm-stat-lbl">Em Negociação</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_s_qual}</div><div class="crm-stat-lbl">Qualificados</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_s_perd}</div><div class="crm-stat-lbl">Perdidos</div></div>
</div>""", unsafe_allow_html=True)

    f1, f2, f3 = st.columns([2, 1, 1])
    with f1:
        busca = st.text_input("Buscar por empresa, commodity ou país", key="crm_busca",
                              placeholder="Ex: ABC Foods, Soja, Egito…")
    with f2:
        filtro_status = st.selectbox("Status", ["Todos","Novo","Em Negociação","Qualificado","Aguardando","Perdido"],
                                      key="crm_filtro_status")
    with f3:
        filtro_fonte = st.selectbox("Origem", ["Todas","WhatsApp","E-mail","LinkedIn","Outro"],
                                     key="crm_filtro_fonte")

    leads = load_crm_leads(limit=200)

    if busca:
        b = busca.lower()
        leads = [l for l in leads if
                 b in (l.get("empresa","") or "").lower() or
                 b in (l.get("commodity","") or "").lower() or
                 b in (l.get("pais_destino","") or "").lower() or
                 b in (l.get("nome","") or "").lower()]
    if filtro_status != "Todos":
        leads = [l for l in leads if l.get("status_lead") == filtro_status]
    if filtro_fonte != "Todas":
        leads = [l for l in leads if l.get("fonte") == filtro_fonte]

    if not leads:
        st.markdown("""
<div class="empty-state">
  <div style="margin-bottom:12px">
    <svg width="32" height="32" fill="none" stroke="#D1D5DB" stroke-width="1.5"
      stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">
      <rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/>
      <line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>
    </svg>
  </div>
  <div class="empty-state-title">Nenhum lead cadastrado ainda</div>
  <div class="empty-state-sub">Cole uma mensagem acima e clique em "Extrair Lead" para começar.</div>
</div>""", unsafe_allow_html=True)
    else:
        st.markdown(f"<div style='font-size:10px;color:#9CA3AF;font-family:Montserrat,sans-serif;"
                    f"margin-bottom:12px'>{len(leads)} lead(s) encontrado(s)</div>",
                    unsafe_allow_html=True)
        for lead in leads:
            status_css = {
                "Novo":          "border-left:3px solid #329632",
                "Em Negociação": "border-left:3px solid #EA580C",
                "Qualificado":   "border-left:3px solid #1D4ED8",
                "Perdido":       "border-left:3px solid #DC2626",
                "Aguardando":    "border-left:3px solid #D1D5DB",
            }.get(lead.get("status_lead",""), "border-left:3px solid #D1D5DB")

            vol_tag   = f'<span class="crm-tag orange">{lead["volume"]}</span>' if lead.get("volume") else ""
            inc_tag   = f'<span class="crm-tag blue">{lead["incoterm"]}</span>'  if lead.get("incoterm") else ""
            comm_tag  = f'<span class="crm-tag green">{lead["commodity"]}</span>' if lead.get("commodity") else ""
            fonte_tag = f'<span class="crm-tag">{lead.get("fonte","")}</span>'   if lead.get("fonte") else ""
            destino   = " → ".join(filter(None, [lead.get("pais_destino",""), lead.get("porto_destino","")]))
            obs       = (f'<div class="crm-obs">{lead["observacoes"][:180]}{"…" if len(lead.get("observacoes",""))>180 else ""}</div>'
                         if lead.get("observacoes") else "")
            created   = (lead.get("created_at","") or "")[:16].replace("T", " ")

            st.markdown(f"""
<div class="crm-card" style="{status_css}">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px">
    <div style="flex:1;min-width:0">
      <div class="crm-title">{lead.get("empresa") or lead.get("nome") or "—"}</div>
      <div class="crm-sub">{lead.get("nome","") or ""}{" · " if lead.get("nome") and destino else ""}{destino}</div>
      <div class="crm-tags">{comm_tag}{vol_tag}{inc_tag}{fonte_tag}{_confianca_tag(lead.get("confianca",""))}</div>
      {obs}
    </div>
    <div style="text-align:right;flex-shrink:0">
      {_status_badge(lead.get("status_lead","Novo"))}
      <div style="font-size:9px;color:#9CA3AF;margin-top:6px;font-family:Montserrat,sans-serif">{created}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)


# ── Tab 2 — Treinamento IA ───────────────────────────────────────────────────
def _tab_treinamento():
    from agents.training_agent import (
        chat_training, get_quick_questions,
        create_faq_table, save_faq,
    )
    create_faq_table()

    st.markdown("""
<div style="font-size:22px;font-weight:900;color:#1a1a1a;margin-bottom:4px;font-family:Montserrat,sans-serif">
🤖 Treinamento com <span style="color:#FA8200">IA</span></div>
<div style="color:#777;font-size:12px;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #e8e8e8;font-family:Montserrat,sans-serif">
Pergunte qualquer coisa sobre commodities, LinkedIn, vendas ou o processo da Samba Export.</div>
""", unsafe_allow_html=True)

    _user_meta = {
        "JA": ("#FCE4EC","#E91E63","Equipe Operacional"),
        "PE": ("#E3F2FD","#1565C0","Equipe Operacional"),
        "CL": ("#E8F5E9","#2e7d32","Equipe Operacional"),
        "ST": ("#F3E5F5","#6A1B9A","Equipe Operacional"),
        "LB": ("#FBE9E7","#E65100","Sócio"),
        "ND": ("#E0F2F1","#00695C","Sócio"),
        "MM": ("#EDE7F6","#4527A0","Sócio"),
    }

    if "train_user_code" not in st.session_state:
        st.session_state["train_user_code"] = "JA"

    # ── Layout: sidebar de usuários + chat ───────────────────────────────────
    sidebar_col, chat_col = st.columns([1, 3], gap="medium")

    with sidebar_col:
        st.markdown('<div class="section-label">Quem está perguntando?</div>', unsafe_allow_html=True)
        for code, name in _USUARIOS.items():
            bg_c, txt_c, role = _user_meta.get(code, ("#f7f7f7","#777","Equipe"))
            is_active = st.session_state.get("train_user_code") == code
            active_cls = "active" if is_active else ""
            st.markdown(f"""
<div class="user-card-sidebar {active_cls}">
  <div class="uca" style="background:{bg_c};color:{txt_c};">{code}</div>
  <div>
    <div class="user-card-name">{name}</div>
    <div class="user-card-role">{role}</div>
  </div>
</div>""", unsafe_allow_html=True)
            if st.button(f"▸ {name}", key=f"train_usr_{code}",
                         help=f"Selecionar {name}", use_container_width=True):
                st.session_state["train_user_code"] = code
                # Reset chat for new user context
                st.rerun()

        st.markdown("<div style='height:12px;border-top:1px solid #e8e8e8;margin-top:4px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-label">💡 Perguntas rápidas</div>', unsafe_allow_html=True)
        _quick_qs = [
            ("cif_fob", "O que é CIF e FOB?"),
            ("sugar", "Lead quer comprar açúcar"),
            ("icumsa", "ICUMSA 45 vs 150"),
            ("noresponse", "Lead não respondeu há 7 dias"),
            ("finder", "O que é Finder?"),
            ("ncda", "O que é NCDA?"),
        ]
        user_code_sb = st.session_state.get("train_user_code","JA")
        _qq_texts = {
            "cif_fob": "O que é CIF e FOB? Explica simples.",
            "sugar": "Recebi uma mensagem de alguém que quer comprar açúcar. O que faço?",
            "icumsa": "O que é ICUMSA 45? Qual a diferença para ICUMSA 150?",
            "noresponse": "O lead não respondeu há 7 dias. O que faço?",
            "finder": "O que é um Finder e como apresento o programa para alguém?",
            "ncda": "O que é NCDA e por que precisa assinar antes de avançar?",
        }
        for qk, qlbl in _quick_qs:
            if st.button(f"▶ {qlbl}", key=f"train_quick_{qk}", use_container_width=True):
                chat_key_q = f"train_chat_{user_code_sb}"
                if chat_key_q not in st.session_state:
                    st.session_state[chat_key_q] = []
                st.session_state[chat_key_q].append({"role": "user", "content": _qq_texts[qk]})
                with st.spinner("Pensando…"):
                    answer = chat_training(st.session_state[chat_key_q])
                st.session_state[chat_key_q].append({"role": "assistant", "content": answer})
                st.rerun()

    with chat_col:
        user_code = st.session_state.get("train_user_code","JA")
        current_name = _USUARIOS.get(user_code, user_code)
        bg_c, txt_c, _ = _user_meta.get(user_code, ("#f7f7f7","#777",""))

        # Chat header — laranja
        st.markdown(f"""
<div style="background:#FA8200;color:#fff;padding:14px 18px;border-radius:12px 12px 0 0;
  font-weight:800;font-size:13px;font-family:Montserrat,sans-serif;
  display:flex;align-items:center;gap:12px">
  <div style="width:36px;height:36px;border-radius:50%;background:rgba(255,255,255,.25);
    display:flex;align-items:center;justify-content:center;font-weight:900;font-size:14px">{user_code}</div>
  <div>
    <div>{current_name} está perguntando</div>
    <div style="font-size:10px;font-weight:400;opacity:.85;margin-top:2px">Assistente Samba Export — responde em português</div>
  </div>
</div>""", unsafe_allow_html=True)

        chat_key = f"train_chat_{user_code}"
        if chat_key not in st.session_state:
            st.session_state[chat_key] = []

        # Histórico do chat
        history = st.session_state[chat_key]
        st.markdown('<div style="background:#fafafa;border:1px solid #e8e8e8;border-top:none;border-radius:0;padding:16px;min-height:300px;max-height:420px;overflow-y:auto">', unsafe_allow_html=True)
        if not history:
            st.markdown("""
<div style="font-family:Montserrat,sans-serif;font-size:12px;color:#2d2d2d;line-height:1.7;
  background:#fff;border:1px solid #e8e8e8;border-radius:12px 12px 12px 3px;padding:12px 16px;max-width:80%">
  Olá! 👋 Sou o assistente de treinamento da <strong>Samba Export</strong>.<br><br>
  Pode me perguntar qualquer coisa sobre:<br>
  • Como responder mensagens no LinkedIn<br>
  • CIF, ICUMSA, SBLC, GMO e outros termos<br>
  • Como qualificar um lead<br>
  • O Programa de Finders<br><br>
  <strong>Fala sem medo! 🎯</strong>
</div>""", unsafe_allow_html=True)
        for i, msg in enumerate(history):
            if msg["role"] == "user":
                st.markdown(f"""
<div style="text-align:right;margin:6px 0 2px">
  <div class="chat-sender">{current_name}</div>
  <div class="chat-bubble-user">{msg["content"]}</div>
</div>""", unsafe_allow_html=True)
            else:
                st.markdown(f"""
<div style="margin:6px 0 2px">
  <div class="chat-sender">Samba Export IA</div>
  <div class="chat-bubble-ai">{msg["content"]}</div>
</div>""", unsafe_allow_html=True)
                user_q = history[i-1]["content"] if i > 0 and history[i-1]["role"] == "user" else ""
                if st.button("💾 Salvar como dúvida", key=f"train_save_faq_{i}"):
                    fid = save_faq(user_code=user_code, question=user_q,
                                   answer=msg["content"], tags="")
                    if fid:
                        st.success(f"Dúvida #{fid} salva!")
                    else:
                        st.error("Erro ao salvar.")
        st.markdown("</div>", unsafe_allow_html=True)

        # Input
        st.markdown('<div style="background:#fff;border:1px solid #e8e8e8;border-top:none;border-radius:0 0 12px 12px;padding:12px 16px">', unsafe_allow_html=True)
        with st.form(key=f"train_form_{user_code}", clear_on_submit=True):
            inp_col, btn_col = st.columns([5, 1])
            with inp_col:
                user_input = st.text_input(
                    "Pergunta",
                    placeholder="Pergunte alguma coisa… (Enter para enviar)",
                    label_visibility="collapsed",
                    key=f"train_input_{user_code}",
                )
            with btn_col:
                submitted = st.form_submit_button("➤ Enviar", use_container_width=True, type="primary")
        st.markdown("</div>", unsafe_allow_html=True)

        if submitted and (user_input or "").strip():
            st.session_state[chat_key].append({"role": "user", "content": user_input.strip()})
            with st.spinner("Samba Export IA está respondendo…"):
                answer = chat_training(st.session_state[chat_key])
            st.session_state[chat_key].append({"role": "assistant", "content": answer})
            st.rerun()

        if history:
            if st.button("🗑️ Limpar conversa", key=f"train_clear_{user_code}"):
                st.session_state[chat_key] = []
                st.rerun()


# ── Tab 3 — Dúvidas da Equipe ────────────────────────────────────────────────
def _tab_duvidas():
    from agents.training_agent import create_faq_table, load_faqs, delete_faq
    create_faq_table()

    st.markdown('<div class="section-label">Base de Dúvidas da Equipe</div>', unsafe_allow_html=True)

    _TAGS_DISPONIVEIS = [
        "Frequente", "Commodity", "Alerta", "Processo",
        "Proteínas", "Automação", "LinkedIn", "Perfil de lead",
    ]

    col_search, col_tag_area = st.columns([2, 3])
    with col_search:
        busca_faq = st.text_input("Buscar por texto", key="faq_busca",
                                   placeholder="Ex: ICUMSA, FOB, comprador…")
    with col_tag_area:
        tag_selecionada = st.selectbox(
            "Filtrar por tag",
            ["Todas"] + _TAGS_DISPONIVEIS,
            key="faq_tag_filter",
        )

    faqs = load_faqs(limit=300)

    if busca_faq:
        b = busca_faq.lower()
        faqs = [f for f in faqs if
                b in (f.get("question","") or "").lower() or
                b in (f.get("answer","") or "").lower()]
    if tag_selecionada != "Todas":
        faqs = [f for f in faqs if tag_selecionada in (f.get("tags","") or "")]

    if not faqs:
        st.markdown("""
<div class="empty-state">
  <div style="margin-bottom:12px">
    <svg width="32" height="32" fill="none" stroke="#D1D5DB" stroke-width="1.5"
      stroke-linecap="round" stroke-linejoin="round" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="10"/>
      <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>
      <line x1="12" y1="17" x2="12.01" y2="17"/>
    </svg>
  </div>
  <div class="empty-state-title">Nenhuma dúvida registrada ainda</div>
  <div class="empty-state-sub">Use o Treinamento IA e clique em "Salvar como dúvida" para preencher esta base.</div>
</div>""", unsafe_allow_html=True)
        return

    st.markdown(f"<div style='font-size:10px;color:#9CA3AF;font-family:Montserrat,sans-serif;"
                f"margin-bottom:12px'>{len(faqs)} dúvida(s) encontrada(s)</div>",
                unsafe_allow_html=True)

    for faq in faqs:
        faq_id   = faq.get("id", 0)
        question = faq.get("question","") or ""
        answer   = faq.get("answer","") or ""
        tags     = faq.get("tags","") or ""
        user_c   = faq.get("user_code","") or ""
        created  = (faq.get("created_at","") or "")[:16].replace("T"," ")

        tags_html = ""
        if tags:
            for t in tags.split(","):
                t = t.strip()
                if t:
                    tags_html += f'<span class="faq-tag">{t}</span>'

        user_name = _USUARIOS.get(user_c, user_c)

        with st.expander(f"{question[:100]}{'…' if len(question)>100 else ''}"):
            st.markdown(f"""
<div style="font-family:Montserrat,sans-serif">
  <div style="font-size:9px;color:#9CA3AF;margin-bottom:8px">
    Por {user_name} · {created}
  </div>
  {f'<div style="margin-bottom:8px">{tags_html}</div>' if tags_html else ""}
  <div style="font-size:12px;color:#111827;line-height:1.6;white-space:pre-wrap">{answer}</div>
</div>""", unsafe_allow_html=True)

            new_tags = st.text_input(
                "Tags (separadas por vírgula)",
                value=tags,
                key=f"faq_tags_{faq_id}",
                placeholder="Ex: Commodity, Processo, Frequente",
            )

            dc1, dc2, _ = st.columns([1, 1, 3])
            with dc1:
                if st.button("Atualizar tags", key=f"faq_update_tags_{faq_id}"):
                    try:
                        from models.database import get_session
                        import sqlalchemy as sa
                        sess = get_session()
                        sess.execute(
                            sa.text("UPDATE faq_questions SET tags=:t WHERE id=:id"),
                            {"t": new_tags, "id": faq_id}
                        )
                        sess.commit()
                        st.success("Tags atualizadas!")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Erro: {exc}")
            with dc2:
                if st.button("Deletar", key=f"faq_del_{faq_id}", type="secondary"):
                    if delete_faq(faq_id):
                        st.success("Dúvida removida.")
                        st.rerun()
                    else:
                        st.error("Erro ao deletar.")


# ── Tab 4 — Lousa (Whiteboard) ───────────────────────────────────────────────
def _tab_lousa():
    from agents.lousa_agent import analisar_texto, get_modos

    st.markdown('<div class="section-label">Lousa — Análise de Textos com IA</div>',
                unsafe_allow_html=True)

    lousa_key = "lousa_history"
    if lousa_key not in st.session_state:
        st.session_state[lousa_key] = {}

    col_u2, col_m, _ = st.columns([1, 1, 2])
    with col_u2:
        lousa_user = st.selectbox(
            "Usuário",
            options=list(_USUARIOS.keys()),
            format_func=lambda k: f"{k} — {_USUARIOS[k]}",
            key="lousa_user_code",
        )
    with col_m:
        modo = st.selectbox("Modo de análise", get_modos(), key="lousa_modo")

    texto_input = st.text_area(
        "Cole aqui qualquer texto para análise",
        height=160,
        key="lousa_texto",
        placeholder="Cole e-mail, mensagem, documento, contrato, notícia de mercado…",
    )

    analisar_btn = st.button("Analisar com IA", key="lousa_analisar", type="primary")

    if analisar_btn:
        if not texto_input.strip():
            st.warning("Cole um texto antes de analisar.")
        else:
            with st.spinner(f"Aplicando '{modo}'…"):
                resultado = analisar_texto(texto_input.strip(), modo)

            user_history = st.session_state[lousa_key].get(lousa_user, [])
            user_history.insert(0, {
                "modo":      modo,
                "entrada":   texto_input.strip()[:200],
                "resultado": resultado,
                "ts":        datetime.datetime.now().strftime("%d/%m %H:%M"),
            })
            st.session_state[lousa_key][lousa_user] = user_history[:5]

            st.markdown('<div class="section-label" style="margin-top:16px">Resultado</div>',
                        unsafe_allow_html=True)
            st.markdown(f"""
<div style="background:#fff;border:1px solid #E5E7EB;border-radius:10px;
  padding:18px 20px;font-family:Montserrat,sans-serif;font-size:12px;
  line-height:1.7;color:#111827;
  box-shadow:0 1px 3px rgba(0,0,0,.06),0 1px 2px rgba(0,0,0,.04);
  white-space:pre-wrap">{resultado}</div>""", unsafe_allow_html=True)

    user_hist = st.session_state[lousa_key].get(lousa_user, [])
    if user_hist:
        st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="section-label">Últimas análises</div>', unsafe_allow_html=True)
        for item in user_hist:
            with st.expander(f"[{item['ts']}] {item['modo']} — {item['entrada'][:60]}…"):
                st.markdown(f"""
<div style="font-family:Montserrat,sans-serif;font-size:12px;line-height:1.7;
  color:#111827;white-space:pre-wrap">{item['resultado']}</div>""",
                            unsafe_allow_html=True)


# ── Tab 5 — Caixa dos Sócios ─────────────────────────────────────────────────
def _create_socios_table():
    try:
        from models.database import get_engine
        import sqlalchemy as sa
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(sa.text("""
                CREATE TABLE IF NOT EXISTS socios_inbox (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    de_user     TEXT    DEFAULT '',
                    para_user   TEXT    DEFAULT '',
                    prioridade  TEXT    DEFAULT 'Normal',
                    mensagem    TEXT    DEFAULT '',
                    status      TEXT    DEFAULT 'pendente',
                    created_at  TEXT    DEFAULT '',
                    updated_at  TEXT    DEFAULT ''
                )
            """))
            conn.commit()
        return True
    except Exception:
        return False


def _tab_socios():
    _create_socios_table()

    _SOCIOS = {"LB": "Leonardo", "ND": "Nívio", "MM": "Marcelo"}
    _PRIORIDADES = ["Normal", "Média", "Urgente"]
    _STATUS_LISTA = ["pendente", "em_andamento", "finalizado"]

    # ── Header laranja ───────────────────────────────────────────────────────
    st.markdown("""
<div style="font-size:22px;font-weight:900;color:#1a1a1a;margin-bottom:4px;font-family:Montserrat,sans-serif">
📥 <span style="color:#FA8200">Caixa</span> dos Sócios</div>
<div style="color:#777;font-size:12px;margin-bottom:16px;padding-bottom:16px;border-bottom:1px solid #e8e8e8;font-family:Montserrat,sans-serif">
Mensagens internas entre a equipe e os sócios (Leonardo, Nívio, Marcelo).</div>
""", unsafe_allow_html=True)

    # ── Painel de composição ─────────────────────────────────────────────────
    if st.session_state.get("socios_compose_open", False):
        st.markdown("""
<div style="background:#f7f7f7;border:1px solid #e8e8e8;border-radius:10px;padding:16px;margin-bottom:16px">
  <div style="font-weight:700;font-size:12px;color:#FA8200;margin-bottom:12px;font-family:Montserrat,sans-serif">
    ✉️ Nova mensagem
  </div>""", unsafe_allow_html=True)
        fc1, fc2, fc3 = st.columns([1, 1, 1])
        with fc1:
            de_code = st.selectbox("De", options=list(_USUARIOS.keys()),
                                   format_func=lambda k: f"{k} — {_USUARIOS[k]}", key="socios_de")
        with fc2:
            para_code = st.selectbox("Para", options=list(_SOCIOS.keys()),
                                     format_func=lambda k: f"{k} — {_SOCIOS[k]}", key="socios_para")
        with fc3:
            prioridade = st.selectbox("Prioridade", _PRIORIDADES, key="socios_prio")

        mensagem = st.text_area("Mensagem", height=90, key="socios_msg",
                                placeholder="Descreva sua mensagem, solicitação ou alerta…")
        sc1, sc2, _ = st.columns([1, 1, 3])
        with sc1:
            if st.button("📤 Enviar", key="socios_enviar", type="primary", use_container_width=True):
                if not mensagem.strip():
                    st.warning("Digite uma mensagem.")
                else:
                    try:
                        from models.database import get_session
                        import sqlalchemy as sa
                        sess = get_session()
                        sess.execute(sa.text("""
                            INSERT INTO socios_inbox
                                (de_user, para_user, prioridade, mensagem, status, created_at, updated_at)
                            VALUES (:de, :para, :prio, :msg, 'pendente', :now, :now)
                        """), {"de": de_code, "para": para_code, "prio": prioridade,
                               "msg": mensagem.strip(), "now": datetime.datetime.now().isoformat()})
                        sess.commit()
                        st.success("✅ Mensagem enviada!")
                        st.session_state["socios_compose_open"] = False
                        st.rerun()
                    except Exception as exc:
                        st.error(f"Erro: {exc}")
        with sc2:
            if st.button("Cancelar", key="socios_cancel", use_container_width=True):
                st.session_state["socios_compose_open"] = False
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        if st.button("✉️ Nova mensagem", key="socios_compose_open_btn", type="primary"):
            st.session_state["socios_compose_open"] = True
            st.rerun()

    # ── Carregar mensagens ────────────────────────────────────────────────────
    try:
        from models.database import get_session
        import sqlalchemy as sa
        sess = get_session()
        rows = sess.execute(sa.text("""
            SELECT id, de_user, para_user, prioridade, mensagem, status, created_at
            FROM socios_inbox ORDER BY id DESC LIMIT 200
        """)).fetchall()
        msgs = [dict(r._mapping) for r in rows]
    except Exception as exc:
        st.error(f"Erro ao carregar inbox: {exc}")
        msgs = []

    # ── Kanban 3 colunas ──────────────────────────────────────────────────────
    _COLS = [
        ("pendente",     "📋 A Fazer",         "#FFF3E0", "#FA8200"),
        ("em_andamento", "⚙️ Em Andamento",      "#E3F2FD", "#1565C0"),
        ("finalizado",   "✅ Finalizados",        "#E8F5E9", "#2e7d32"),
    ]
    _PRIO_CLS = {"Urgente":"p-urgente","Média":"p-media","Normal":"p-normal"}
    _PRIO_BADGE = {"Urgente":"pb-urgente","Média":"pb-media","Normal":"pb-normal"}

    kanban_c1, kanban_c2, kanban_c3 = st.columns(3, gap="small")
    for col_widget, (status_key, col_title, col_bg, col_color) in zip(
        [kanban_c1, kanban_c2, kanban_c3], _COLS
    ):
        filtered = [m for m in msgs if m.get("status") == status_key]
        with col_widget:
            st.markdown(f"""
<div>
  <div class="kanban-header" style="background:{col_bg};color:{col_color}">
    {col_title}
    <span style="background:{col_color};color:#fff;border-radius:12px;padding:2px 8px;font-size:10px">{len(filtered)}</span>
  </div>
  <div class="kanban-body">""", unsafe_allow_html=True)

            if not filtered:
                st.markdown('<div class="kanban-empty">Nenhuma mensagem</div>', unsafe_allow_html=True)
            else:
                for msg in filtered:
                    mid    = msg.get("id", 0)
                    de_n   = _USUARIOS.get(msg.get("de_user",""), msg.get("de_user","?"))
                    para_n = _SOCIOS.get(msg.get("para_user",""), msg.get("para_user","?"))
                    prio   = msg.get("prioridade","Normal")
                    texto  = msg.get("mensagem","")
                    created= (msg.get("created_at","") or "")[:16].replace("T"," ")
                    prio_cls = _PRIO_CLS.get(prio, "p-normal")
                    pbadge   = _PRIO_BADGE.get(prio, "pb-normal")
                    st.markdown(f"""
<div class="kanban-card {prio_cls}">
  <div style="font-size:11px;font-weight:700;color:#1a1a1a;margin-bottom:5px;font-family:Montserrat,sans-serif;line-height:1.5">{texto[:120]}{"…" if len(texto)>120 else ""}</div>
  <div style="font-size:9px;color:#777;font-family:Montserrat,sans-serif">
    {de_n} → {para_n} &nbsp;·&nbsp;
    <span class="prio-badge {pbadge}">{prio}</span>&nbsp;·&nbsp;{created}
  </div>
</div>""", unsafe_allow_html=True)

                    outros_status = [s for s in _STATUS_LISTA if s != status_key]
                    btn_cols_k = st.columns(len(outros_status))
                    for bi, novo_status in enumerate(outros_status):
                        btn_lbl_k = {
                            "em_andamento": "▶ Iniciar",
                            "finalizado": "✓ Finalizar",
                            "pendente": "↩ Reabrir",
                        }.get(novo_status, novo_status)
                        with btn_cols_k[bi]:
                            if st.button(btn_lbl_k, key=f"socios_k_{mid}_{novo_status}",
                                         use_container_width=True):
                                try:
                                    from models.database import get_session
                                    import sqlalchemy as sa
                                    sess2 = get_session()
                                    sess2.execute(sa.text("""
                                        UPDATE socios_inbox
                                        SET status=:s, updated_at=:now WHERE id=:id
                                    """), {"s": novo_status,
                                           "now": datetime.datetime.now().isoformat(),
                                           "id": mid})
                                    sess2.commit()
                                    st.rerun()
                                except Exception as exc:
                                    st.error(f"Erro: {exc}")
            st.markdown("</div></div>", unsafe_allow_html=True)


# ── Tab 6 — Ref. Documentos ──────────────────────────────────────────────────
def _tab_docs_ref():
    _DOCS = [
        {
            "nome": "COA — Certificate of Analysis",
            "categoria": "Qualidade",
            "cat_cls": "cat-qualidade",
            "descricao": (
                "Certificado emitido pelo laboratório ou fornecedor atestando as especificações "
                "técnicas do produto (pureza, umidade, impurezas, aflatoxinas, etc.). "
                "Essencial para açúcar ICUMSA, soja e milho. Deve acompanhar o embarque. "
                "Parâmetros verificados: ICUMSA, umidade, impurezas, polarização (para açúcar). "
                "Emitido antes ou no dia do embarque e deve coincidir com o Laudo de Qualidade."
            ),
        },
        {
            "nome": "CRF — Certificado de Registro de Fornecedor",
            "categoria": "Exportação",
            "cat_cls": "cat-exportacao",
            "descricao": (
                "Documento emitido pelo MAPA (Ministério da Agricultura) habilitando o exportador "
                "a comercializar produtos agrícolas no exterior. Exigido especialmente para "
                "proteínas animais (frango, suíno). Validade periódica — deve ser renovado."
            ),
        },
        {
            "nome": "Packing List",
            "categoria": "Logística",
            "cat_cls": "cat-logistica",
            "descricao": (
                "Lista de embalagem que detalha o conteúdo físico do embarque: número de sacas/bags, "
                "peso bruto, peso líquido, dimensões, número de lote e marcas de identificação. "
                "Deve estar alinhado com a Commercial Invoice e o BL. "
                "Fundamental para o desembaraço aduaneiro no destino."
            ),
        },
        {
            "nome": "BL — Bill of Lading",
            "categoria": "Logística",
            "cat_cls": "cat-logistica",
            "descricao": (
                "Conhecimento de embarque marítimo — o título de propriedade da carga. "
                "Emitido pela armadora (Navio). Contém: shipper (exportador), consignee (comprador), "
                "porto de origem, porto de destino, descrição da carga, incoterm e frete. "
                "Existem 3 vias originais negociáveis. Fundamental para liberar a carga no destino."
            ),
        },
        {
            "nome": "Commercial Invoice",
            "categoria": "Financeiro",
            "cat_cls": "cat-financeiro",
            "descricao": (
                "Fatura comercial emitida pelo exportador ao comprador. Deve conter: "
                "dados completos das partes, descrição do produto, quantidade, preço unitário, "
                "valor total, incoterm, condições de pagamento e referência do contrato (SPA/LOI). "
                "Base para o cálculo do imposto de importação no país de destino."
            ),
        },
        {
            "nome": "KYC — Know Your Customer",
            "categoria": "Compliance",
            "cat_cls": "cat-compliance",
            "descricao": (
                "Procedimento de diligência do comprador/vendedor. Documentos solicitados: "
                "certidão de constituição da empresa, comprovante de endereço, documentos dos sócios, "
                "referências bancárias e comerciais. Obrigatório antes de assinar SPA ou LOI. "
                "Protege a Samba Export de fraudes e lavagem de dinheiro."
            ),
        },
        {
            "nome": "NCDA — Non-Circumvention Agreement",
            "categoria": "Jurídico",
            "cat_cls": "cat-juridico",
            "descricao": (
                "Acordo de Não Circumvenção e Não Divulgação. Protege o intermediador (Finder/Broker) "
                "de ser cortado da negociação. Assinado entre Samba Export e o parceiro antes de "
                "revelar detalhes do vendedor ou comprador. Contém: prazo de validade, penalidades, "
                "cláusula de não circunvenção por 2-5 anos. Também chamado de NCNDA quando inclui NDA."
            ),
        },
        {
            "nome": "SISCOMEX",
            "categoria": "Governo",
            "cat_cls": "cat-governo",
            "descricao": (
                "Sistema Integrado de Comércio Exterior do governo federal brasileiro. "
                "Plataforma obrigatória para registro de exportações. Gera o RE (Registro de Exportação), "
                "DU-E (Declaração Única de Exportação) e Nota Fiscal de Exportação. "
                "Operado via Portal Único do Comércio Exterior (PUCOMEX)."
            ),
        },
        {
            "nome": "SGS / Bureau Veritas / Intertek",
            "categoria": "Inspeção",
            "cat_cls": "cat-inspecao",
            "descricao": (
                "Empresas certificadoras internacionais que realizam inspeção da carga antes do embarque. "
                "Verificam: quantidade (pesagem), qualidade (análise laboratorial), condição do navio "
                "(hold inspection) e conformidade com o contrato. O laudo emitido por essas empresas "
                "é aceito internacionalmente e reduz disputas comerciais."
            ),
        },
        {
            "nome": "Laudo de Qualidade",
            "categoria": "Inspeção",
            "cat_cls": "cat-inspecao",
            "descricao": (
                "Documento técnico emitido por laboratório credenciado ou inspetor independente "
                "atestando os parâmetros físico-químicos da carga no momento do embarque. "
                "Deve coincidir com o COA. Parâmetros: umidade, impurezas, peso específico, "
                "proteína (soja), polarização (açúcar). Base para aceite ou reclamação pelo comprador."
            ),
        },
        {
            "nome": "SPA — Sales and Purchase Agreement",
            "categoria": "Jurídico",
            "cat_cls": "cat-juridico",
            "descricao": (
                "Contrato de Compra e Venda — o documento jurídico definitivo da operação. "
                "Substitui a LOI após negociação. Contém: partes, commodity, volume, preço, "
                "incoterm, porto de embarque/destino, prazo de embarque, condições de pagamento "
                "(LC, TT, CAD), especificações técnicas, penalidades e lei aplicável. "
                "Deve ser revisado por advogado antes da assinatura."
            ),
        },
    ]

    _CATEGORIAS = sorted(set(d["categoria"] for d in _DOCS))

    st.markdown('<div class="section-label">Biblioteca de Documentos de Exportação</div>',
                unsafe_allow_html=True)

    # Filtro por categoria — pills clicáveis via selectbox (compatível com Streamlit)
    filtro_cat = st.selectbox(
        "Filtrar por categoria",
        ["Todas"] + _CATEGORIAS,
        key="docref_filtro_cat",
    )

    docs_filtrados = _DOCS if filtro_cat == "Todas" else [
        d for d in _DOCS if d["categoria"] == filtro_cat
    ]

    st.markdown(f"<div style='font-size:10px;color:#9CA3AF;font-family:Montserrat,sans-serif;"
                f"margin-bottom:14px'>{len(docs_filtrados)} documento(s)</div>",
                unsafe_allow_html=True)

    # Grid 2 colunas
    for i in range(0, len(docs_filtrados), 2):
        col_a, col_b = st.columns(2)
        for col, doc in zip([col_a, col_b], docs_filtrados[i:i+2]):
            with col:
                st.markdown(f"""
<div class="doc-ref-card">
  <span class="doc-cat-badge {doc['cat_cls']}">{doc['categoria']}</span>
  <div class="doc-ref-name">{doc['nome']}</div>
  <div class="doc-ref-desc">{doc['descricao']}</div>
</div>""", unsafe_allow_html=True)


# ── Tab 7 — Anonimizador ─────────────────────────────────────────────────────
def _tab_anonimizador():
    import io
    import numpy as np

    st.markdown("""
<div style="font-size:22px;font-weight:900;color:#1a1a1a;margin-bottom:4px;font-family:Montserrat,sans-serif">
🔒 <span style="color:#FA8200">Anonimizador</span> — Imagens & PDFs</div>
<div style="color:#777;font-size:12px;margin-bottom:14px;padding-bottom:14px;border-bottom:1px solid #e8e8e8;font-family:Montserrat,sans-serif">
Carregue uma imagem (PNG/JPG) ou um PDF. <strong>Arraste para desenhar blocos pretos</strong> sobre qualquer área sensível. Processado 100% localmente.</div>
""", unsafe_allow_html=True)

    # ── Dependências ──────────────────────────────────────────────────────────
    try:
        from streamlit_drawable_canvas import st_canvas
    except ImportError:
        st.error("Instale o componente de canvas: **pip install streamlit-drawable-canvas**")
        return
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        st.error("Instale Pillow: **pip install Pillow**")
        return

    # ── Upload ────────────────────────────────────────────────────────────────
    uploaded = st.file_uploader(
        "Arraste ou clique para carregar (PNG, JPG ou PDF)",
        type=["png", "jpg", "jpeg", "pdf"],
        key="anon_upload",
        help="Processado 100% localmente",
    )

    if not uploaded:
        st.markdown("""
<div style="border:2px dashed #e8e8e8;border-radius:12px;padding:52px 24px;text-align:center;
  background:#fafafa;font-family:Montserrat,sans-serif">
  <div style="font-size:44px;margin-bottom:12px">🔒</div>
  <div style="font-weight:700;font-size:15px;color:#2d2d2d;margin-bottom:8px">
    Arraste um arquivo aqui ou clique em "Browse files"</div>
  <div style="font-size:12px;color:#777;line-height:1.8">
    PNG · JPG · PDF (suporte a múltiplas páginas)</div>
  <div style="margin-top:16px;display:flex;gap:10px;justify-content:center;flex-wrap:wrap">
    <span style="background:#FFF3E0;color:#FA8200;font-size:10px;font-weight:700;
      padding:4px 12px;border-radius:20px;font-family:Montserrat,sans-serif">
      ✏️ Arraste para desenhar blocos pretos</span>
    <span style="background:#E8F5E9;color:#2e7d32;font-size:10px;font-weight:700;
      padding:4px 12px;border-radius:20px;font-family:Montserrat,sans-serif">
      🔒 100% local — sem upload</span>
  </div>
</div>""", unsafe_allow_html=True)
        return

    is_pdf    = uploaded.name.lower().endswith(".pdf")
    file_bytes = uploaded.read()
    base_name  = uploaded.name.rsplit(".", 1)[0]

    # ── Preparar imagem (página) para o canvas ────────────────────────────────
    # Canvas width fixo para caber na tela — escalamos a imagem para essa largura
    CANVAS_W = 860

    if is_pdf:
        try:
            import fitz
        except ImportError:
            st.error("Instale PyMuPDF: **pip install pymupdf**")
            return

        doc     = fitz.open(stream=file_bytes, filetype="pdf")
        n_pages = len(doc)

        # Controles topo
        ctrl_c1, ctrl_c2, ctrl_c3 = st.columns([1, 1, 3])
        with ctrl_c1:
            page_num = int(st.number_input(
                "Página", min_value=1, max_value=n_pages,
                value=1, step=1, key="anon_pdf_page")) - 1
        with ctrl_c2:
            render_dpi = st.selectbox(
                "Qualidade", [72, 100, 150], index=1,
                format_func=lambda d: f"{d} DPI", key="anon_dpi")

        page   = doc[page_num]
        pw_pts = page.rect.width    # largura em pontos PDF
        ph_pts = page.rect.height   # altura em pontos PDF

        # Renderizar página
        scale  = render_dpi / 72.0
        mat    = fitz.Matrix(scale, scale)
        pix    = page.get_pixmap(matrix=mat)
        img    = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGBA")

        st.markdown(f"""
<div style="background:#FFF3E0;border:1px solid #FFD180;border-radius:8px;padding:9px 16px;
  font-family:Montserrat,sans-serif;font-size:12px;color:#7B3800;margin-bottom:12px;
  display:flex;align-items:center;gap:10px">
  <span style="font-size:18px">📄</span>
  <span><strong>{uploaded.name}</strong> · {n_pages} pág · página {page_num+1}
  · {int(pw_pts)}×{int(ph_pts)} pt</span>
</div>""", unsafe_allow_html=True)

    else:
        doc     = None
        n_pages = 1
        page_num = 0
        pw_pts  = None
        img     = Image.open(io.BytesIO(file_bytes)).convert("RGBA")

        st.markdown(f"""
<div style="background:#FFF3E0;border:1px solid #FFD180;border-radius:8px;padding:9px 16px;
  font-family:Montserrat,sans-serif;font-size:12px;color:#7B3800;margin-bottom:12px;
  display:flex;align-items:center;gap:10px">
  <span style="font-size:18px">🖼️</span>
  <span><strong>{uploaded.name}</strong> · {img.width}×{img.height} px</span>
</div>""", unsafe_allow_html=True)

    # ── Escalar imagem para o canvas ──────────────────────────────────────────
    orig_w, orig_h = img.size
    scale_factor   = CANVAS_W / orig_w
    canvas_h       = int(orig_h * scale_factor)
    img_resized    = img.resize((CANVAS_W, canvas_h), Image.LANCZOS)

    # ── Instrução de uso ──────────────────────────────────────────────────────
    st.markdown("""
<div style="background:#f7f7f7;border:1px solid #e8e8e8;border-radius:8px;padding:10px 16px;
  font-family:Montserrat,sans-serif;font-size:12px;color:#2d2d2d;margin-bottom:12px;
  display:flex;align-items:center;gap:10px">
  <span style="font-size:20px">✏️</span>
  <span><strong>Clique e arraste</strong> para desenhar retângulos pretos sobre áreas sensíveis.
  Desenhe quantos quiser. Quando terminar, clique em <strong>"Exportar"</strong>.</span>
</div>""", unsafe_allow_html=True)

    # ── Canvas interativo ─────────────────────────────────────────────────────
    canvas_key = f"anon_canvas_{page_num}_{base_name}"
    canvas_result = st_canvas(
        fill_color   = "rgba(0, 0, 0, 1)",      # preenchimento preto opaco
        stroke_width = 0,
        stroke_color = "#000000",
        background_image = img_resized,
        update_streamlit = True,
        height       = canvas_h,
        width        = CANVAS_W,
        drawing_mode = "rect",
        display_toolbar = True,
        key          = canvas_key,
    )

    # ── Extrair retângulos desenhados ─────────────────────────────────────────
    drawn_rects = []
    if canvas_result and canvas_result.json_data:
        for obj in canvas_result.json_data.get("objects", []):
            if obj.get("type") == "rect":
                # Coordenadas no canvas (pixels escalados)
                cx = float(obj.get("left",   0))
                cy = float(obj.get("top",    0))
                cw = float(obj.get("width",  0)) * float(obj.get("scaleX", 1))
                ch = float(obj.get("height", 0)) * float(obj.get("scaleY", 1))
                drawn_rects.append({"cx": cx, "cy": cy, "cw": cw, "ch": ch})

    # Contador de blocos
    n_blocos = len(drawn_rects)
    if n_blocos > 0:
        st.markdown(f"""
<div style="background:#E8F5E9;border-radius:8px;padding:8px 14px;font-family:Montserrat,sans-serif;
  font-size:12px;color:#2e7d32;font-weight:700;margin-top:8px">
  ✅ {n_blocos} bloco{'s' if n_blocos>1 else ''} desenhado{'s' if n_blocos>1 else ''}
  — clique em "Exportar" para aplicar permanentemente
</div>""", unsafe_allow_html=True)

    # ── Exportar ──────────────────────────────────────────────────────────────
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)
    exp_c1, exp_c2 = st.columns([3, 1])
    with exp_c1:
        st.markdown("""
<div style="font-family:Montserrat,sans-serif;font-size:11px;color:#777;line-height:1.7">
  Os blocos pretos são <strong>permanentes</strong> no arquivo exportado.
  Guarde o original antes de exportar. O canvas aceita undo (Ctrl+Z) antes de exportar.</div>""",
        unsafe_allow_html=True)
    with exp_c2:
        exportar = st.button(
            "⬛ Exportar anonimizado",
            key="anon_exportar",
            type="primary",
            use_container_width=True,
            disabled=(n_blocos == 0),
        )

    if exportar and n_blocos > 0:
        if is_pdf and doc is not None:
            # ── Exportar PDF: converter coords canvas → pontos PDF ────────────
            import fitz
            for pg_idx in range(n_pages):
                pg = doc[pg_idx]
                if pg_idx != page_num:
                    continue  # só aplica na página atual do canvas
                for r in drawn_rects:
                    # canvas px → original px
                    ox = r["cx"] / scale_factor
                    oy = r["cy"] / scale_factor
                    ow = r["cw"] / scale_factor
                    oh = r["ch"] / scale_factor
                    # original px → PDF pts (considerando render_dpi)
                    pts_per_px = 72.0 / (render_dpi if is_pdf else 72)
                    rx = ox * pts_per_px
                    ry = oy * pts_per_px
                    rw = ow * pts_per_px
                    rh = oh * pts_per_px
                    rect = fitz.Rect(rx, ry, rx + rw, ry + rh)
                    pg.add_redact_annot(rect, fill=(0, 0, 0))
                pg.apply_redactions()

            out_buf = io.BytesIO()
            doc.save(out_buf, garbage=4, deflate=True)
            out_buf.seek(0)
            st.success(f"✅ {n_blocos} bloco{'s' if n_blocos>1 else ''} aplicado{'s' if n_blocos>1 else ''} permanentemente no PDF.")
            st.download_button(
                label=f"⬇️ Baixar {base_name}_anonimizado.pdf",
                data=out_buf,
                file_name=f"{base_name}_anonimizado.pdf",
                mime="application/pdf",
                key="anon_dl_pdf",
                type="primary",
                use_container_width=True,
            )

        else:
            # ── Exportar imagem: desenhar sobre original ──────────────────────
            img_out = Image.open(io.BytesIO(file_bytes)).convert("RGB")
            draw    = ImageDraw.Draw(img_out)
            for r in drawn_rects:
                # canvas px → original px
                ox = int(r["cx"] / scale_factor)
                oy = int(r["cy"] / scale_factor)
                ow = int(r["cw"] / scale_factor)
                oh = int(r["ch"] / scale_factor)
                draw.rectangle([ox, oy, ox+ow, oy+oh], fill="black")

            out_buf = io.BytesIO()
            img_out.save(out_buf, format="PNG")
            out_buf.seek(0)
            st.success(f"✅ {n_blocos} bloco{'s' if n_blocos>1 else ''} aplicado{'s' if n_blocos>1 else ''}.")
            st.download_button(
                label=f"⬇️ Baixar {base_name}_anonimizado.png",
                data=out_buf,
                file_name=f"{base_name}_anonimizado.png",
                mime="image/png",
                key="anon_dl_img",
                type="primary",
                use_container_width=True,
            )

    # ── Nota multi-página PDF ─────────────────────────────────────────────────
    if is_pdf and n_pages > 1:
        st.markdown(f"""
<div style="background:#FFF3E0;border:1px solid #FFD180;border-radius:8px;padding:10px 14px;
  margin-top:14px;font-family:Montserrat,sans-serif;font-size:11px;color:#7B3800">
  📄 <strong>PDF com {n_pages} páginas:</strong> O canvas mostra uma página por vez.
  Troque a página no seletor acima, desenhe os blocos e exporte cada página separadamente.
  Para redatar múltiplas páginas de uma vez, use a abordagem por coordenadas manuais
  ou exporte cada página e junte os PDFs depois.
</div>""", unsafe_allow_html=True)


# ── Tab SCRIPTS — Scripts prontos A-J com EN+PT e exemplos práticos ──────────
def _tab_scripts():
    st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FFF7ED;border:1px solid #FED7AA;border-left:4px solid #FA8200;border-radius:8px;padding:12px 16px;margin-bottom:16px">
<strong style="font-size:12px;color:#78350F">⛔ Regra de ouro:</strong>
<span style="font-size:11px;color:#92400E"> Nunca improvise os scripts. Copie e cole exatamente. Substitua apenas o que está entre [colchetes].
Os scripts em inglês são para enviar ao lead. O português é apenas para você entender o que está enviando.</span>
</div>""", unsafe_allow_html=True)

    # ── PERFIL 1 — ENGAJADOR ─────────────────────────────────────────────────
    st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#EFF6FF;border:1px solid #BFDBFE;border-left:4px solid #1D4ED8;border-radius:8px;padding:10px 16px;margin:4px 0 10px">
<div style="font-size:9px;font-weight:800;letter-spacing:2px;color:#1D4ED8;text-transform:uppercase;margin-bottom:3px">PERFIL 1 — ENGAJADOR / POTENCIAL FINDER</div>
<div style="font-size:11px;color:#1E3A8A">Pessoa que interagiu com post (curtiu, comentou, compartilhou). NÃO é comprador direto. Pode conhecer um comprador.</div>
</div>""", unsafe_allow_html=True)

    with st.expander("Script G — Pitch Finder · Para quem curtiu, comentou ou compartilhou"):
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FEFCE8;border:1px solid #FDE047;border-radius:6px;padding:8px 12px;font-size:11px;color:#713F12;margin-bottom:10px">
📌 <strong>Quando usar:</strong> Pessoa que interagiu com algum post. Ela <em>pode conhecer</em> um comprador — recrutar para o Programa Finder.</div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#166534;background:#F0FDF4;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇺🇸 INGLÊS — ENVIAR ASSIM</div>', unsafe_allow_html=True)
            st.code("Hi [Nome]! I noticed you engaged with our content — thank you for that.\nWe run a formal Finder Program: if you know serious active buyers in commodities, we pay 10–30% commission on closed deals, for up to 36 months.\nZero investment. Zero conflict of interest. Fully backed by legal contract.\nWould this be of interest to you?", language=None)
        with c2:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#1D4ED8;background:#EFF6FF;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇧🇷 PORTUGUÊS — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
            st.code("Olá [Nome]! Vi que você interagiu com nosso conteúdo — obrigado por isso.\nTemos um Programa de Finders formal: se você conhece compradores sérios ativos em commodities, pagamos comissão de 10% a 30% em negócios fechados, por até 36 meses.\nZero investimento. Zero conflito de interesses. Tudo formalizado em contrato.\nIsso seria de seu interesse?", language=None)

    # ── PERFIL 2 — COMPRADOR ─────────────────────────────────────────────────
    st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FEF2F2;border:1px solid #FECACA;border-left:4px solid #DC2626;border-radius:8px;padding:10px 16px;margin:16px 0 10px">
<div style="font-size:9px;font-weight:800;letter-spacing:2px;color:#DC2626;text-transform:uppercase;margin-bottom:3px">PERFIL 2 — COMPRADOR / MANDATÁRIO</div>
<div style="font-size:11px;color:#7F1D1D">Importador direto, trader internacional ou empresa que compra commodities. Qualificar e migrar para WhatsApp.</div>
</div>""", unsafe_allow_html=True)

    with st.expander("⭐ Script D — 3 Perguntas OBRIGATÓRIAS antes de qualquer proposta", expanded=True):
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FEF2F2;border:1px solid #FECACA;border-radius:6px;padding:8px 12px;font-size:11px;color:#7F1D1D;margin-bottom:10px">
⛔ <strong>NUNCA pule este script.</strong> Antes de qualquer PDF, preço ou proposta — faça essas 3 perguntas.<br>
Erro clássico: mandar 7 PDFs sem qualificar (caso Tejinder Singh). <strong>Script D primeiro. Sempre.</strong></div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#166534;background:#F0FDF4;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇺🇸 INGLÊS — ENVIAR ASSIM</div>', unsafe_allow_html=True)
            st.code("Perfect — we have active supply for [COMMODITY].\nTo prepare an indicative offer, I need just 3 quick details:\n1. Approximate volume? (MT or bags)\n2. Destination port?\n3. Preferred terms — CIF or FOB?\nWould it be easier to continue on WhatsApp?", language=None)
        with c2:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#1D4ED8;background:#EFF6FF;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇧🇷 PORTUGUÊS — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
            st.code("Perfeito — temos abastecimento ativo de [COMMODITY].\nPara preparar uma oferta indicativa, preciso de apenas 3 informações rápidas:\n1. Volume aproximado? (MT ou sacas)\n2. Porto de destino?\n3. Condição preferida — CIF ou FOB?\nSeria mais prático continuar pelo WhatsApp?", language=None)

    with st.expander("Script A — Pós-Conexão Ativa (nós pedimos, ele aceitou)"):
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FEFCE8;border:1px solid #FDE047;border-radius:6px;padding:8px 12px;font-size:11px;color:#713F12;margin-bottom:10px">
📌 <strong>Quando usar:</strong> Você enviou o pedido de conexão, ele aceitou. Enviar em até 24h. Não esperar ele falar primeiro.</div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#166534;background:#F0FDF4;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇺🇸 INGLÊS — ENVIAR ASSIM</div>', unsafe_allow_html=True)
            st.code("Hi [Nome]! Thank you for connecting.\nWe operate as originators and exporters of Brazilian commodities on a large scale, with structured execution from origin to shipment.\nI'm seeking relationships with direct buyers and mandates who have active import demand.\nIf this is of interest to you, I'd be glad to have a quick conversation.", language=None)
        with c2:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#1D4ED8;background:#EFF6FF;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇧🇷 PORTUGUÊS — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
            st.code("Olá [Nome]! Obrigado por aceitar a conexão.\nSomos originadores e exportadores de commodities brasileiras em larga escala, com execução estruturada da origem até o embarque.\nBusco relações com compradores diretos e mandatários que tenham demanda ativa de importação.\nSe isso for de seu interesse, fico à disposição para uma conversa rápida.", language=None)

    with st.expander("Script B — Lead conecta e fala primeiro"):
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FEFCE8;border:1px solid #FDE047;border-radius:6px;padding:8px 12px;font-size:11px;color:#713F12;margin-bottom:10px">
📌 <strong>Quando usar:</strong> O lead aceitou e mandou uma mensagem, OU ele nos adicionou e falou primeiro.<br>
💡 <strong>Dica:</strong> Sempre terminar com uma pergunta direta. Objetivo: identificar se é comprador ou intermediário.</div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#166534;background:#F0FDF4;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇺🇸 INGLÊS — ENVIAR ASSIM</div>', unsafe_allow_html=True)
            st.code("Hi [Nome]! Great to connect.\nWe are Brazilian commodity originators and exporters — large scale, full compliance, competitive CIF worldwide.\nAre you a buyer or do you represent import demand for any specific commodity?", language=None)
        with c2:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#1D4ED8;background:#EFF6FF;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇧🇷 PORTUGUÊS — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
            st.code("Olá [Nome]! Ótimo conectar.\nSomos originadores e exportadores brasileiros de commodities — larga escala, compliance completo, preços CIF competitivos para todo o mundo.\nVocê é comprador direto ou representa demanda de importação para alguma commodity específica?", language=None)

    with st.expander("Script E — Migração para WhatsApp (lead qualificado)"):
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#F0FDF4;border:1px solid #BBF7D0;border-radius:6px;padding:8px 12px;font-size:11px;color:#14532D;margin-bottom:10px">
✅ <strong>Quando usar:</strong> Lead confirmou interesse real. Respondeu às 3 perguntas do Script D.<br>
💡 <strong>Grupo WhatsApp:</strong> Criar como "Samba x [Nome do Lead]". Incluir Leonardo + Marcelo + Nívio.</div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#166534;background:#F0FDF4;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇺🇸 INGLÊS — ENVIAR ASSIM</div>', unsafe_allow_html=True)
            st.code("Great — let's move this forward on WhatsApp for faster communication.\nCould you share your number? I'll add you to our commercial group with our team.", language=None)
        with c2:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#1D4ED8;background:#EFF6FF;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇧🇷 PORTUGUÊS — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
            st.code("Ótimo — vamos acelerar no WhatsApp para comunicação mais rápida.\nPode compartilhar seu número? Vou te adicionar ao nosso grupo comercial com nossa equipe.", language=None)

    with st.expander("Script F — Follow-up (lead sumiu por 7+ dias)"):
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FEF2F2;border:1px solid #FECACA;border-radius:6px;padding:8px 12px;font-size:11px;color:#7F1D1D;margin-bottom:10px">
⛔ <strong>Apenas UM follow-up.</strong> Se não responder, status no CRM: "Frio — aguardar". Não insistir mais.</div>""", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#166534;background:#F0FDF4;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇺🇸 INGLÊS — ENVIAR ASSIM</div>', unsafe_allow_html=True)
            st.code("Hi [Nome] — just following up on our last conversation.\nWe still have active supply available. If the timing is right, happy to pick up where we left off.", language=None)
        with c2:
            st.markdown('<div style="font-size:9px;font-weight:700;color:#1D4ED8;background:#EFF6FF;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇧🇷 PORTUGUÊS — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
            st.code("Olá [Nome] — retomando nossa conversa anterior.\nAinda temos abastecimento ativo disponível. Se o momento for oportuno, podemos continuar de onde paramos.", language=None)

    # ── EXEMPLOS PRÁTICOS ────────────────────────────────────────────────────
    st.markdown('<div class="section-label" style="margin-top:24px">Exemplos Práticos — Como Compradores Reais Escrevem</div>', unsafe_allow_html=True)
    st.markdown("""<div style="font-family:Montserrat,sans-serif;font-size:11px;color:#6B7280;margin-bottom:12px">
Mensagens reais de compradores, tradução, especificações e como responder corretamente.</div>""", unsafe_allow_html=True)

    exemplos = [
        {
            "titulo": "🍬 Açúcar ICUMSA 45 — Arábia Saudita",
            "original": "We are looking for Sugar ICUMSA 45, 10,000 MT monthly, CIF Jeddah Saudi Arabia. Please send your best offer with full specs.",
            "traducao": "Procuramos Açúcar ICUMSA 45, 10.000 toneladas por mês, com entrega CIF no porto de Jeddah, Arábia Saudita. Por favor enviar a melhor oferta com especificações completas.",
            "specs": "Polarização ≥ 99,80°Z · Umidade ≤ 0,04%\nICUMSA ≤ 45 UI · Sulfatos ≤ 2ppm\nOrigem: Brasil (Bonsucro/Halal certificado)",
            "alerta": "⚠️ Confirmar se é comprador final ou mandatário. Perguntar instrumento de pagamento. Não mandar preço antes disso.",
            "resposta_en": "Thank you for reaching out.\nWe supply Sugar ICUMSA 45 from certified Brazilian mills — Bonsucro / Halal certified, available CIF ASWP.\n\nBefore preparing your indicative price, quick confirm:\n- Are you the end buyer or a mandatary?\n- Payment instrument: DLC MT700 or SBLC MT760?\n- Monthly contract (12 months) or spot trial first?\n\nOnce confirmed, FCO ready within 24 hours.",
            "resposta_pt": "Obrigado pelo contato.\nFornecemos Açúcar ICUMSA 45 de usinas brasileiras certificadas — Bonsucro / Halal, disponível CIF ASWP.\n\nAntes de preparar o preço indicativo, confirmar rapidamente:\n- Você é comprador final ou mandatário?\n- Instrumento de pagamento: DLC MT700 ou SBLC MT760?\n- Contrato mensal (12 meses) ou primeiro pedido teste?\n\nConfirmado, FCO pronto em até 24 horas.",
        },
        {
            "titulo": "🌾 Feijão Preto — Portugal / Europa",
            "original": "Hello, we need black beans 500 MT for Brazil to Portugal, FOB Santos. Can you quote?",
            "traducao": "Olá, precisamos de feijão preto 500 MT, Brasil para Portugal, FOB Santos. Pode cotar?",
            "specs": "Umidade máx 14% · Impurezas máx 1%\nGrãos quebrados máx 3% · Tipo 1 ou 3\nDG SANTE registrado (Europa)",
            "alerta": "⚠️ 500 MT = pedido de teste. Verificar registro UE (DG SANTE). Pagamento: 30% adiantado + 70% antes embarque.",
            "resposta_en": "Hello — we supply Black Beans and Carioca Beans, certified for EU markets (DG SANTE registered).\n\nFor your 500 MT trial, FOB Santos:\n- Caliber 1 / type 3 available\n- Free of Salmonella, Aflatoxins under 10ppb (SGS)\n- Payment: 30% advance + 70% before BL\n\nCan you confirm: Is the buyer EU-registered for food imports?\nIndicative FOB offer within 24 hours.",
            "resposta_pt": "Olá — fornecemos Feijão Preto e Carioca, certificados para o mercado europeu (DG SANTE).\n\nPara seu pedido teste de 500 MT, FOB Santos:\n- Calibre 1 / tipo 3 disponível\n- Livre de Salmonela, Aflatoxinas abaixo de 10ppb (SGS)\n- Pagamento: 30% adiantado + 70% antes do embarque\n\nPode confirmar: o comprador tem registro UE para importação de alimentos?\nOferta indicativa FOB em 24 horas.",
        },
        {
            "titulo": "🍚 Arroz Branco — Angola",
            "original": "Good day. We need White Rice 5% broken, 3,000 MT, CIF Luanda Angola. Urgent.",
            "traducao": "Bom dia. Precisamos de Arroz Branco 5% quebrados, 3.000 MT, CIF Luanda Angola. Urgente.",
            "specs": "Umidade máx 14% · Quebrados máx 5%\nGrãos gessados máx 2% · Impurezas máx 0,5%\nCertificação MAPA (Brasil)",
            "alerta": "⚠️ Angola exige licença MIREX de importação. 'Urgente' não muda o processo — qualificar antes de acelerar.",
            "resposta_en": "Good day — we supply White Rice Long Grain, 5% broken, from certified Brazilian mills.\n\nFor 3,000 MT CIF Luanda:\n- Origin: Rio Grande do Sul / Mato Grosso\n- Inspection: SGS at loading port\n- Phytosanitary + MAPA certification included\n\nBefore the price:\n1. Do you hold a valid MIREX import license?\n2. Payment: DLC MT700 or 30%+70% TT?\n3. Spot order or monthly contract?\n\nIndicative price in 24 hours after confirmation.",
            "resposta_pt": "Bom dia — fornecemos Arroz Branco Longo Fino, 5% quebrados, de moinhos certificados.\n\nPara 3.000 MT CIF Luanda:\n- Origem: Rio Grande do Sul / Mato Grosso\n- Inspeção: SGS no porto de carregamento\n- Fitossanitário + certificação MAPA incluídos\n\nAntes do preço:\n1. Possui licença MIREX de importação válida?\n2. Pagamento: DLC MT700 ou 30%+70% TT?\n3. Pedido spot ou contrato mensal?\n\nPreço indicativo em 24 horas após confirmação.",
        },
    ]
    for ex in exemplos:
        with st.expander(ex["titulo"]):
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f'<div style="font-family:Montserrat,sans-serif;font-size:10px;font-weight:700;color:#DC2626;margin-bottom:4px">📩 O que o lead escreveu</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="background:#F5F5F5;border:1px solid #E5E7EB;border-radius:6px;padding:10px;font-size:11px;font-family:monospace;color:#374151;margin-bottom:10px">{ex["original"]}</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="font-family:Montserrat,sans-serif;font-size:10px;font-weight:700;color:#1D4ED8;margin-bottom:4px">📩 Tradução — o que ele quer</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="background:#EFF6FF;border:1px solid #BFDBFE;border-radius:6px;padding:10px;font-size:11px;color:#1E3A8A">{ex["traducao"]}</div>', unsafe_allow_html=True)
            with c2:
                st.markdown(f'<div style="font-family:Montserrat,sans-serif;font-size:10px;font-weight:700;color:#166534;margin-bottom:4px">📋 Especificações do produto</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:6px;padding:10px;font-size:11px;font-family:monospace;color:#14532D;margin-bottom:10px;white-space:pre-line">{ex["specs"]}</div>', unsafe_allow_html=True)
                st.markdown(f'<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:6px;padding:8px 10px;font-size:10px;color:#7F1D1D">{ex["alerta"]}</div>', unsafe_allow_html=True)
            st.markdown('<div style="font-family:Montserrat,sans-serif;font-size:10px;font-weight:700;color:#374151;margin:10px 0 4px">✉️ Como responder</div>', unsafe_allow_html=True)
            c3, c4 = st.columns(2)
            with c3:
                st.markdown('<div style="font-size:9px;font-weight:700;color:#166534;background:#F0FDF4;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇺🇸 RESPOSTA — ENVIAR ASSIM</div>', unsafe_allow_html=True)
                st.code(ex["resposta_en"], language=None)
            with c4:
                st.markdown('<div style="font-size:9px;font-weight:700;color:#1D4ED8;background:#EFF6FF;border-radius:4px;padding:3px 8px;margin-bottom:4px;display:inline-block">🇧🇷 TRADUÇÃO — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
                st.code(ex["resposta_pt"], language=None)
    st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#F0FDF4;border:1px solid #BBF7D0;border-radius:8px;padding:10px 14px;font-size:11px;color:#14532D;margin-top:16px">
📌 <strong>Lembrete:</strong> Os preços reais são sempre confirmados por Nívio, Marcelo ou Leonardo.
Sua função é qualificar, filtrar e passar adiante — não precisa saber o preço.</div>""", unsafe_allow_html=True)


# ── Tab MAPA & FILTRO — Mapa de leads, situações, funil e filtro comprador ────
def _tab_mapa_filtro():
    # ── Perfis de lead ────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Tipos de Lead — Como Identificar</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#EFF6FF;border:1px solid #BFDBFE;border-top:4px solid #1D4ED8;border-radius:8px;padding:14px 16px;height:100%">
<div style="font-size:9px;font-weight:800;letter-spacing:2px;color:#1D4ED8;text-transform:uppercase;margin-bottom:8px">👤 PERFIL 1 — ENGAJADOR</div>
<div style="font-size:11px;color:#1E3A8A;line-height:1.8"><strong>Quem é:</strong> Agrônomo, consultor, corretor de grãos, despachante<br>
<strong>Como chegou:</strong> Curtiu, comentou ou compartilhou um post<br>
<strong>É comprador?</strong> <span style="color:#DC2626;font-weight:700">NÃO</span> — mas pode INDICAR um<br><br>
<div style="background:#DBEAFE;border-radius:6px;padding:8px"><strong>→ OBJETIVO:</strong> Recrutar para o Programa Finder<br><strong>→ Script:</strong> Script G (Pitch Finder)</div></div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown("""<div style="font-family:Montserrat,sans-serif;background:#FEF2F2;border:1px solid #FECACA;border-top:4px solid #DC2626;border-radius:8px;padding:14px 16px;height:100%">
<div style="font-size:9px;font-weight:800;letter-spacing:2px;color:#DC2626;text-transform:uppercase;margin-bottom:8px">👤 PERFIL 2 — COMPRADOR / MANDATÁRIO</div>
<div style="font-size:11px;color:#7F1D1D;line-height:1.8"><strong>Quem é:</strong> Importador direto, trader, empresa que compra<br>
<strong>Como chegou:</strong> Pediu conexão ou nós pedimos<br>
<strong>É comprador?</strong> <span style="color:#16A34A;font-weight:700">SIM (ou representa um)</span><br><br>
<div style="background:#FEE2E2;border-radius:6px;padding:8px"><strong>→ OBJETIVO:</strong> Qualificar e migrar para WhatsApp<br><strong>→ Scripts:</strong> A, B, C, D, E</div></div></div>""", unsafe_allow_html=True)

    # ── 8 situações ───────────────────────────────────────────────────────────
    st.markdown('<div class="section-label" style="margin-top:20px">8 Situações — Como o Lead Pode Chegar</div>', unsafe_allow_html=True)
    _sits = [
        ("CURTIU UM POST", "PERFIL 1", "#1D4ED8", "#EFF6FF", "Aparece como 'curtiu' ou 'reagiu' na publicação", "→ Enviar convite de conexão + Script G depois"),
        ("COMENTOU OU COMPARTILHOU", "PERFIL 1", "#1D4ED8", "#EFF6FF", "Engajamento maior = prioridade máxima.", "→ Conectar + Script G imediatamente"),
        ("PEDIU CONEXÃO PARA NÓS", "PERFIL 2", "#DC2626", "#FEF2F2", "Alguém nos encontrou e adicionou. Tem intenção.", "→ Aceitar e usar Script C. Responder em até 24h."),
        ("NÓS PEDIMOS CONEXÃO (pós-ativa)", "PERFIL 2", "#DC2626", "#FEF2F2", "Enviamos convite e ele aceitou → Conexão Pós-Ativa", "→ Script A imediatamente (não esperar ele falar)"),
        ("RESPONDEU COM INTERESSE", "PERFIL 2", "#DC2626", "#FEF2F2", "Pediu produto, preço ou mais informações", "→ Script D. Fazer 3 perguntas ANTES de qualquer PDF."),
        ("NÃO RESPONDEU (7+ dias)", "QUALQUER", "#6B7280", "#F9FAFB", "Silêncio após primeiro contato", "→ Script F — 1 follow-up. Se não responder: FRIO."),
        ("RESPONDEU MAS NÃO COMPRA", "PERFIL 1", "#1D4ED8", "#EFF6FF", "O produto não serve para ele diretamente", "→ Script G — Pitch Finder antes de encerrar"),
        ("QUALIFICADO ✅", "PERFIL 2", "#16A34A", "#F0FDF4", "Confirmou commodity + volume + porto de destino", "→ Script E — Migrar para WhatsApp + NCDA automático"),
    ]
    cols = st.columns(2)
    for i, (title, profile, color, bg, desc, action) in enumerate(_sits):
        with cols[i % 2]:
            st.markdown(f"""<div style="background:{bg};border:1px solid {color}33;border-left:3px solid {color};border-radius:8px;padding:11px 14px;margin-bottom:8px;font-family:Montserrat,sans-serif">
<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:5px">
<strong style="font-size:11px;color:#111827">{title}</strong>
<span style="font-size:9px;font-weight:700;background:{color};color:#fff;padding:2px 7px;border-radius:20px;margin-left:8px;white-space:nowrap">{profile}</span>
</div>
<div style="font-size:10px;color:#6B7280;margin-bottom:5px">{desc}</div>
<div style="font-size:11px;font-weight:700;color:{color}">{action}</div>
</div>""", unsafe_allow_html=True)

    # ── Funil ─────────────────────────────────────────────────────────────────
    st.markdown('<div class="section-label" style="margin-top:20px">Funil Completo — Do LinkedIn ao Contrato</div>', unsafe_allow_html=True)
    _funil = [
        ("01", "#1D4ED8", "#DBEAFE", "LinkedIn — Identificar e Qualificar", "Máximo 2-3 mensagens. Usar scripts."),
        ("02", "#1D4ED8", "#DBEAFE", "Propor WhatsApp", 'Script E — "Could you share your number?"'),
        ("03", "#FA8200", "#FFF7ED", "Criar Grupo WhatsApp", '"Samba x [Nome]" — Leonardo + Marcelo + Nívio'),
        ("04", "#FA8200", "#FFF7ED", "NCDA via ZapSign (automático)", "Contrato enviado automaticamente. Lead assina pelo celular."),
        ("05", "#16A34A", "#F0FDF4", "NCDA Assinado ✅ — Sócios assumem", "FCO → SPA → Instrumento financeiro."),
        ("06", "#16A34A", "#F0FDF4", "Negociação e Fechamento", "Leonardo + Nívio + Marcelo cuidam de tudo aqui."),
        ("07", "#16A34A", "#F0FDF4", "Receita 💰", "Negócio fechado. Samba executa. Finder recebe."),
    ]
    for idx, (num, color, bg, title, detail) in enumerate(_funil):
        br_top = "8px 0 0 0" if idx == 0 else "0"
        br_bot = "0 0 0 8px" if idx == len(_funil)-1 else "0"
        br_r   = "0 8px 8px 0" if idx in (0, len(_funil)-1) else "0"
        st.markdown(f"""<div style="display:flex;margin-bottom:2px;font-family:Montserrat,sans-serif">
<div style="min-width:46px;background:{color};display:flex;align-items:center;justify-content:center;font-weight:900;font-size:12px;color:#fff;border-radius:{br_top} {br_bot};padding:12px 0">{num}</div>
<div style="flex:1;background:{bg};border:1px solid rgba(0,0,0,0.05);border-left:none;padding:11px 16px;border-radius:0 {br_r};display:flex;justify-content:space-between;align-items:center;gap:12px">
<strong style="font-size:12px;color:#111827">{title}</strong>
<span style="font-size:10px;color:#6B7280;text-align:right;flex-shrink:0;max-width:260px">{detail}</span>
</div></div>""", unsafe_allow_html=True)

    # ── Filtro comprador ──────────────────────────────────────────────────────
    st.markdown('<div class="section-label" style="margin-top:24px">Filtro do Comprador — 10 Perguntas antes de qualquer proposta</div>', unsafe_allow_html=True)
    st.markdown('<div style="font-family:Montserrat,sans-serif;font-size:11px;color:#6B7280;margin-bottom:14px">Registre o que você já sabe sobre o lead. Não é obrigatório perguntar tudo de uma vez — algumas você descobre pelo perfil LinkedIn.</div>', unsafe_allow_html=True)
    _perguntas = [
        ("01", "Qual é o nome da empresa e o site?", "Precisamos verificar se a empresa é real e qual é o porte.", "Nome da empresa e URL..."),
        ("02", "A empresa já importa atualmente?", "Quem já importa tem processo e banco. Quem nunca importou tem mais risco.", "Sim / Não — quais produtos?"),
        ("03", "Para onde vai a carga? (país + porto)", "Define tudo: certificações, documentação, preço e logística.", "Ex: Índia, porto de Mumbai"),
        ("04", "O lead é comprador final ou intermediário?", "Comprador final fecha direto. Mandatário representa outra empresa.", "Comprador final / Mandatário / Broker"),
        ("05", "Qual commodity e especificação?", "Soja GMO ou Non-GMO? Açúcar ICUMSA 45 ou 150? Os detalhes mudam o preço.", "Ex: Açúcar ICUMSA 45, soja GMO..."),
        ("06", "Qual volume? (toneladas ou sacas)", "Abaixo de 1.000 MT = pedido de teste. Acima de 5.000 MT = contrato anual.", "Ex: 5.000 MT/mês"),
        ("07", "Condição: CIF ou FOB?", "CIF = Samba paga o frete. FOB = o comprador paga o frete.", "CIF / FOB / outros"),
        ("08", "Como ele vai pagar? (instrumento financeiro)", "DLC MT700 ou SBLC MT760 = pagamento garantido. Sem isso = sem negócio.", "DLC / SBLC / TT 30+70..."),
        ("09", "Qual prazo de entrega esperado?", "Alinha expectativa com o calendário de safra e logística.", "Ex: 30 dias, agosto 2026..."),
        ("10", "Tem empresa verificável no LinkedIn/site?", "Empresa sem presença digital = sinal de alerta. Checar antes de avançar.", "Sim / Não — link do site ou LinkedIn"),
    ]
    for num, q, hint, ph in _perguntas:
        st.markdown(f"""<div style="font-family:Montserrat,sans-serif;background:#fff;border:1px solid #E5E7EB;border-left:4px solid #FA8200;border-radius:6px;padding:10px 14px;margin-bottom:6px">
<div style="display:flex;gap:8px;align-items:flex-start;margin-bottom:3px">
<span style="background:#FA8200;color:#fff;font-size:9px;font-weight:800;padding:2px 7px;border-radius:4px;flex-shrink:0">{num}</span>
<strong style="font-size:12px;color:#111827">{q}</strong>
</div>
<div style="font-size:10px;color:#9CA3AF;padding-left:32px;margin-bottom:6px">📌 {hint}</div>
</div>""", unsafe_allow_html=True)
        st.text_input("", placeholder=ph, key=f"filtro_{num}", label_visibility="collapsed")

    # ── Alertas de risco ──────────────────────────────────────────────────────
    st.markdown('<div class="section-label" style="margin-top:20px">Sinais de Alerta — Leads Problemáticos</div>', unsafe_allow_html=True)
    _risks = [
        ("ALTO RISCO", "#DC2626", "#FEF2F2", "#FECACA", "Diz ter 'mandato de governo' sem apresentar documento", "→ Pedir carta de mandato ANTES de qualquer avanço"),
        ("ALTO RISCO", "#DC2626", "#FEF2F2", "#FECACA", "Quer fechar sem NCDA ou sem instrumento financeiro", "→ Regra absoluta: sem NCDA assinado, sem negociação"),
        ("ALTO RISCO", "#DC2626", "#FEF2F2", "#FECACA", "Empresa sem site, sem CNPJ/registro verificável", "→ Não avançar. Due diligence. Passar para Nívio ou Marcelo."),
        ("MÉDIO RISCO", "#EA580C", "#FFF7ED", "#FED7AA", "Pede preço sem informar volume, porto ou condição", "→ Usar Script D. Pedir 3 informações antes de qualquer oferta."),
        ("MÉDIO RISCO", "#EA580C", "#FFF7ED", "#FED7AA", "Responde só 'interested' sem detalhar", "→ Continuar qualificando com Script D. Não assumir interesse real."),
    ]
    for level, color, bg, border, situation, action in _risks:
        st.markdown(f"""<div style="background:{bg};border:1px solid {border};border-radius:8px;padding:10px 14px;margin-bottom:7px;font-family:Montserrat,sans-serif">
<span style="background:{color};color:#fff;font-size:9px;font-weight:800;padding:2px 8px;border-radius:4px;display:inline-block;margin-bottom:5px">{level}</span><br>
<strong style="font-size:12px;color:#111827">{situation}</strong><br>
<span style="font-size:11px;color:#6B7280">{action}</span>
</div>""", unsafe_allow_html=True)

    # ── Finder vs Comprador ───────────────────────────────────────────────────
    st.markdown('<div class="section-label" style="margin-top:20px">Finder vs. Comprador — Diferença em 30 segundos</div>', unsafe_allow_html=True)
    _rows = [
        ("Indica um contato", "É o contato que compra"),
        ("Não assina nada no negócio", "Assina LOI e SPA"),
        ("Não negocia preço", "Recebe proposta e negocia"),
        ("Recebe comissão (10–30%)", "Paga pela carga"),
        ("Permanece sigiloso", "Entra no contrato principal"),
    ]
    header = '<div style="font-family:Montserrat,sans-serif;display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#E5E7EB;border-radius:8px;overflow:hidden">'
    header += '<div style="background:#1D4ED8;color:#fff;font-size:10px;font-weight:700;padding:8px 12px;letter-spacing:.5px">FINDER (ENGAJADOR)</div>'
    header += '<div style="background:#DC2626;color:#fff;font-size:10px;font-weight:700;padding:8px 12px;letter-spacing:.5px">COMPRADOR</div>'
    for i, (f, c) in enumerate(_rows):
        bg = "#fff" if i % 2 == 0 else "#F9FAFB"
        header += f'<div style="background:{bg};font-size:11px;color:#374151;padding:8px 12px">{f}</div>'
        header += f'<div style="background:{bg};font-size:11px;color:#374151;padding:8px 12px">{c}</div>'
    header += "</div>"
    st.markdown(header, unsafe_allow_html=True)


# ── Tab 8 — WhatsApp / Automações ────────────────────────────────────────────
def _tab_whatsapp():
    st.markdown("""
<div class="em-breve-wrap">
  <div class="em-breve-label">Em desenvolvimento</div>
  <div class="em-breve-title">WhatsApp / Automações</div>
  <div class="em-breve-desc">
    Integração via Make.com para captura automática de leads do WhatsApp Business.
    Mensagens recebidas serão processadas automaticamente pelo agente CRM e
    inseridas na base sem intervenção manual. NCNDA distribuída automaticamente
    via ZapSign ao aceite do contato.
  </div>
  <div style="margin-top:24px;display:flex;gap:8px;justify-content:center;flex-wrap:wrap">
    <span class="feature-pill" style="background:#F0FDF4;color:#329632">Make.com Webhook</span>
    <span class="feature-pill" style="background:#FFF7ED;color:#EA580C">ZapSign API</span>
    <span class="feature-pill" style="background:#EFF6FF;color:#1D4ED8">WhatsApp Business</span>
  </div>
</div>""", unsafe_allow_html=True)


# ── Tab 9 — Prospecção ────────────────────────────────────────────────────────
def _tab_prospeccao():
    st.markdown("""
<div class="em-breve-wrap">
  <div class="em-breve-label">Em desenvolvimento</div>
  <div class="em-breve-title">Pipeline de Prospecção</div>
  <div class="em-breve-desc">
    Kanban visual de prospects por estágio (Novo → Qualificado → Proposta →
    Negociação → Fechado). Integração com Notion CRM para sincronização
    bidirecional. Score automático de leads baseado em volume, commodity
    e comportamento de resposta.
  </div>
  <div style="margin-top:24px;display:flex;gap:8px;justify-content:center;flex-wrap:wrap">
    <span class="feature-pill" style="background:#F9FAFB;color:#6B7280">Kanban Board</span>
    <span class="feature-pill" style="background:#F9FAFB;color:#6B7280">Notion Sync</span>
    <span class="feature-pill" style="background:#F9FAFB;color:#6B7280">Lead Scoring</span>
  </div>
</div>""", unsafe_allow_html=True)


# ── Tab Due Diligence ────────────────────────────────────────────────────────
def _tab_due_diligence():
    st.markdown("""
<div style="font-size:22px;font-weight:900;color:#1a1a1a;margin-bottom:4px;font-family:Montserrat,sans-serif">
🔍 <span style="color:#FA8200">Due Diligence</span> — Verificação de Contatos</div>
<div style="color:#777;font-size:12px;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #e8e8e8;font-family:Montserrat,sans-serif">
Checklist de verificação antes de avançar com qualquer lead ou parceiro.</div>
""", unsafe_allow_html=True)

    dd_col1, dd_col2 = st.columns([1, 2], gap="medium")

    with dd_col1:
        st.markdown('<div class="section-label">Nova verificação</div>', unsafe_allow_html=True)
        dd_nome = st.text_input("Nome do contato / empresa", key="dd_nome",
                                placeholder="Ex: Rahul Kumar / IndiaGrains Ltd")
        dd_pais = st.text_input("País", key="dd_pais", placeholder="Ex: Índia, Egito, China…")
        dd_commodity = st.text_input("Commodity de interesse", key="dd_commodity",
                                     placeholder="Ex: Açúcar ICUMSA 45, Soja GMO…")
        dd_volume = st.text_input("Volume declarado", key="dd_volume",
                                  placeholder="Ex: 5.000 MT/mês")
        dd_inst = st.selectbox("Instrumento financeiro declarado",
                               ["Não informado","DLC MT700","SBLC MT760","TT 30+70","TT 100%","Outro"],
                               key="dd_inst")
        dd_linkedin = st.text_input("LinkedIn / site da empresa", key="dd_linkedin",
                                    placeholder="https://linkedin.com/in/...")
        if st.button("📋 Gerar Checklist", key="dd_gerar", type="primary", use_container_width=True):
            st.session_state["dd_result"] = {
                "nome": dd_nome, "pais": dd_pais, "commodity": dd_commodity,
                "volume": dd_volume, "inst": dd_inst, "linkedin": dd_linkedin,
            }
            st.rerun()

    with dd_col2:
        dd_r = st.session_state.get("dd_result")
        if not dd_r:
            st.markdown("""
<div style="text-align:center;padding:48px 24px;background:#fff;border:1px solid #e8e8e8;border-radius:12px">
  <div style="font-size:48px;margin-bottom:16px">🔍</div>
  <div style="font-size:14px;font-weight:700;color:#2d2d2d;margin-bottom:8px;font-family:Montserrat,sans-serif">
    Preencha os dados à esquerda e clique em "Gerar Checklist"</div>
  <div style="font-size:12px;color:#777;font-family:Montserrat,sans-serif;line-height:1.7">
    O checklist ajuda a identificar riscos antes de avançar com o lead.</div>
</div>""", unsafe_allow_html=True)
        else:
            # Score calculado baseado nos dados
            _score = 0
            _max   = 6
            if dd_r.get("nome"): _score += 1
            if dd_r.get("pais"): _score += 1
            if dd_r.get("commodity"): _score += 1
            if dd_r.get("volume") and dd_r.get("volume") != "": _score += 1
            if dd_r.get("inst") and dd_r.get("inst") != "Não informado": _score += 1
            if dd_r.get("linkedin"): _score += 1
            _pct = int((_score / _max) * 100)
            _score_color = "verde" if _pct >= 70 else "amarelo" if _pct >= 40 else "vermelho"
            _score_hex   = {"verde":"#2E7D32","amarelo":"#E65100","vermelho":"#B71C1C"}[_score_color]
            _score_bg    = {"verde":"#E8F5E9","amarelo":"#FFF3E0","vermelho":"#FFEBEE"}[_score_color]
            _risk_label  = {"verde":"BAIXO RISCO","amarelo":"RISCO MÉDIO","vermelho":"ALTO RISCO"}[_score_color]

            st.markdown(f"""
<div style="display:flex;align-items:center;gap:20px;background:#fff;border:1px solid #e8e8e8;
  border-radius:12px;padding:20px 22px;margin-bottom:14px">
  <div style="width:80px;height:80px;border-radius:50%;border:4px solid {_score_hex};
    background:{_score_bg};display:flex;align-items:center;justify-content:center;
    font-size:24px;font-weight:900;color:{_score_hex};flex-shrink:0">{_pct}%</div>
  <div>
    <div style="font-size:16px;font-weight:900;color:#1a1a1a;font-family:Montserrat,sans-serif">
      {dd_r.get("nome","—")}</div>
    <div style="font-size:12px;color:#777;font-family:Montserrat,sans-serif;margin-top:2px">
      {dd_r.get("pais","")} · {dd_r.get("commodity","")}</div>
    <span style="background:{_score_hex};color:#fff;font-size:10px;font-weight:800;
      padding:3px 10px;border-radius:12px;display:inline-block;margin-top:6px;
      font-family:Montserrat,sans-serif">{_risk_label}</span>
  </div>
</div>""", unsafe_allow_html=True)

            # Checklist items
            _checklists = [
                ("empresa_verificavel", "Empresa verificável (site/LinkedIn/CNPJ)",
                 bool(dd_r.get("linkedin")), "Buscar no LinkedIn, site oficial e Google.", True),
                ("volume_realista", "Volume declarado é realista",
                 bool(dd_r.get("volume")), "< 1.000 MT = teste. > 5.000 MT = contrato anual.", False),
                ("instrumento_financeiro", "Instrumento financeiro aceitável",
                 dd_r.get("inst","Não informado") != "Não informado",
                 "DLC MT700 ou SBLC MT760 são os aceitos pela Samba Export.", True),
                ("pais_ok", "País de destino operacional",
                 bool(dd_r.get("pais")), "Verificar lista de países permitidos pelo MAPA e Receita Federal.", False),
                ("ncda_status", "NCDA / NCNDA disponível para assinar",
                 False, "Preparar via ZapSign antes de revelar fornecedores ou compradores.", True),
                ("kyc_status", "KYC iniciado",
                 False, "Solicitar: certidão da empresa, comprovante de endereço, dados dos sócios.", False),
            ]
            st.markdown('<div class="section-label" style="margin-top:6px">Checklist de Verificação</div>', unsafe_allow_html=True)
            for chk_key, chk_lbl, chk_done, chk_hint, chk_required in _checklists:
                icon    = "✅" if chk_done else "⚠️" if chk_required else "○"
                bg_c    = "#E8F5E9" if chk_done else "#FFEBEE" if chk_required else "#fff"
                brd_c   = "#2e7d32" if chk_done else "#c62828" if chk_required else "#e8e8e8"
                with st.expander(f"{icon} {chk_lbl}"):
                    st.markdown(f"""
<div style="font-family:Montserrat,sans-serif;font-size:12px;color:#2d2d2d;line-height:1.7;
  padding:4px 0">
  <div style="background:{bg_c};border-left:3px solid {brd_c};border-radius:4px;
    padding:8px 12px;margin-bottom:8px">{chk_hint}</div>
  <div style="font-size:10px;color:#777">
    {'✅ Informação presente' if chk_done else '⚠️ Pendente — verificar antes de avançar' if chk_required else '○ Opcional / em progresso'}
  </div>
</div>""", unsafe_allow_html=True)

            # Recomendação final
            if _pct >= 70:
                st.success(f"✅ Score {_pct}% — Lead com informações suficientes para avançar. Próximo passo: enviar NCDA.")
            elif _pct >= 40:
                st.warning(f"⚠️ Score {_pct}% — Risco médio. Solicite mais informações antes de revelar fornecedores.")
            else:
                st.error(f"🚨 Score {_pct}% — Alto risco. Não avançar sem verificação completa. Passar para Nívio ou Marcelo.")

            if st.button("🗑️ Limpar resultado", key="dd_limpar"):
                del st.session_state["dd_result"]
                st.rerun()


# ── Entry point ───────────────────────────────────────────────────────────────
def render_comercial_hub():
    # CommonMark termina um bloco <style> na primeira linha em branco.
    # Removemos linhas vazias para que o parser não quebre o bloco CSS.
    _css_compact = "\n".join(ln for ln in _CSS.splitlines() if ln.strip())
    st.markdown(_css_compact, unsafe_allow_html=True)

    import base64 as _b64
    logo_path = ROOT / "assets" / "logo.png"
    _logo_tag = ""
    if logo_path.exists():
        _logo_b64 = _b64.b64encode(logo_path.read_bytes()).decode()
        _logo_tag = (f'<img src="data:image/png;base64,{_logo_b64}" '
                     f'style="height:42px;width:auto;flex-shrink:0;display:block">')

    # ── Botão portal — CSS injetado via _CSS principal (compactado) ─────────────
    # O CSS do botão está no bloco _CSS com seletores múltiplos para Streamlit 1.56.
    if st.button("⌂", key="com_portal_btn", help="Voltar ao Portal"):
        st.session_state.current_view = "portal"
        st.rerun()

    st.markdown(f"""
<div class="com-hdr">
  {_logo_tag if _logo_tag else '<div class="com-hdr-logo"><span>samba</span>EXPORT</div>'}
  <div class="com-hdr-tags" style="margin-left:8px">
    <span class="com-htag">ORGANIZAÇÃO + IA</span>
    <span class="com-htag ai">⚡ CLAUDE</span>
  </div>
  <div style="flex:1"></div>
  <div style="display:flex;gap:8px;align-items:center;flex-shrink:0">
    <a href="https://drive.google.com/drive/folders/0AOllQoxhuNj4Uk9PVA" target="_blank"
       class="com-hdr-btn" title="Google Drive Corporativo">
      <svg fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"
        stroke-linejoin="round" viewBox="0 0 24 24">
        <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/>
      </svg>
    </a>
    <div style="text-align:right;font-family:Montserrat,sans-serif;flex-shrink:0">
      <div style="font-size:9px;letter-spacing:1.8px;color:#777;font-weight:700;text-transform:uppercase">DATA</div>
      <div style="font-weight:800;color:#FA8200;font-size:13px;margin-top:1px">
        {datetime.datetime.now().strftime('%d/%m/%Y')}
      </div>
    </div>
  </div>
</div>
<div style="height:20px"></div>
""", unsafe_allow_html=True)

    # ── KPIs ─────────────────────────────────────────────────────────────────
    try:
        from agents.crm_agent import load_crm_leads, create_crm_table
        create_crm_table()
        all_leads = load_crm_leads(limit=500)
        total     = len(all_leads)
        novos     = sum(1 for l in all_leads if l.get("status_lead") == "Novo")
        em_neg    = sum(1 for l in all_leads if l.get("status_lead") == "Em Negociação")
        qualif    = sum(1 for l in all_leads if l.get("status_lead") == "Qualificado")
    except Exception:
        total = novos = em_neg = qualif = 0

    kc1, kc2, kc3, kc4 = st.columns(4)
    _KPI_DATA = [
        (kc1, "Total Leads",   total,  "na base",              True),
        (kc2, "Novos",         novos,  "aguardando contato",   False),
        (kc3, "Em Negociação", em_neg, "em andamento",         False),
        (kc4, "Qualificados",  qualif, "prontos para proposta",False),
    ]
    for col, label, value, sub, highlight in _KPI_DATA:
        with col:
            hl_cls = "highlight" if highlight else ""
            st.markdown(f"""
<div class="kpi-card {hl_cls}">
  <div class="kpi-label">{label}</div>
  <div class="kpi-value">{value}</div>
  <div class="kpi-sub">{sub}</div>
</div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)

    # ── Abas — 12 tabs ────────────────────────────────────────────────────────
    tabs = st.tabs([
        "📋 Organização",
        "🤖 Treinamento IA",
        "💬 Dúvidas da Equipe",
        "📊 CRM",
        "🔍 Due Diligence",
        "📜 Scripts",
        "🗺️ Mapa & Filtro",
        "🖊️ Lousa",
        "📥 Caixa dos Sócios",
        "📄 Ref. Documentos",
        "🔒 Anonimizador",
        "⚙️ Automações",
    ])

    _pad = "<div style='height:14px'></div>"
    # Tab order: Organização, Treinamento, Dúvidas, CRM, Due Diligence,
    #            Scripts, Mapa & Filtro, Lousa, Caixa Sócios, Ref. Docs,
    #            Anonimizador, Automações
    with tabs[0]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_crm()

    with tabs[1]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_treinamento()

    with tabs[2]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_duvidas()

    with tabs[3]:
        st.markdown(_pad, unsafe_allow_html=True)
        # CRM standalone list view
        try:
            from agents.crm_agent import create_crm_table, load_crm_leads
            create_crm_table()
        except Exception as e:
            st.error(f"Erro CRM: {e}")
            return

        st.markdown("""
<div style="font-size:22px;font-weight:900;color:#1a1a1a;margin-bottom:4px;font-family:Montserrat,sans-serif">
📊 <span style="color:#FA8200">CRM</span> — Base de Leads</div>
<div style="color:#777;font-size:12px;margin-bottom:20px;padding-bottom:16px;border-bottom:1px solid #e8e8e8;font-family:Montserrat,sans-serif">
Todos os leads cadastrados. Use a aba Organização para extrair novos leads.</div>
""", unsafe_allow_html=True)
        all_leads_c = load_crm_leads(limit=500)
        _sc_total = len(all_leads_c)
        _sc_novo  = sum(1 for l in all_leads_c if l.get("status_lead") == "Novo")
        _sc_neg   = sum(1 for l in all_leads_c if l.get("status_lead") == "Em Negociação")
        _sc_qual  = sum(1 for l in all_leads_c if l.get("status_lead") == "Qualificado")
        _sc_perd  = sum(1 for l in all_leads_c if l.get("status_lead") == "Perdido")
        st.markdown(f"""
<div class="crm-stats-bar">
  <div class="crm-stat-item"><div class="crm-stat-num">{_sc_total}</div><div class="crm-stat-lbl">Total</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_sc_novo}</div><div class="crm-stat-lbl">Novos</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_sc_neg}</div><div class="crm-stat-lbl">Em Negociação</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_sc_qual}</div><div class="crm-stat-lbl">Qualificados</div></div>
  <div class="crm-stat-item"><div class="crm-stat-num">{_sc_perd}</div><div class="crm-stat-lbl">Perdidos</div></div>
</div>""", unsafe_allow_html=True)
        cf1, cf2, cf3 = st.columns([2, 1, 1])
        with cf1:
            busca_c = st.text_input("Buscar", key="crm_tab_busca", placeholder="Empresa, commodity, país…")
        with cf2:
            filt_s = st.selectbox("Status", ["Todos","Novo","Em Negociação","Qualificado","Aguardando","Perdido"], key="crm_tab_status")
        with cf3:
            filt_f = st.selectbox("Origem", ["Todas","WhatsApp","E-mail","LinkedIn","Outro"], key="crm_tab_fonte")

        leads_c = all_leads_c
        if busca_c:
            b = busca_c.lower()
            leads_c = [l for l in leads_c if any(b in (l.get(k,"") or "").lower() for k in ("empresa","commodity","pais_destino","nome"))]
        if filt_s != "Todos":
            leads_c = [l for l in leads_c if l.get("status_lead") == filt_s]
        if filt_f != "Todas":
            leads_c = [l for l in leads_c if l.get("fonte") == filt_f]

        if not leads_c:
            st.markdown("""<div class="empty-state"><div class="empty-state-title">Nenhum lead encontrado</div>
<div class="empty-state-sub">Ajuste os filtros ou extraia novos leads na aba Organização.</div></div>""", unsafe_allow_html=True)
        else:
            st.markdown(f"<div style='font-size:10px;color:#9CA3AF;font-family:Montserrat,sans-serif;margin-bottom:12px'>{len(leads_c)} lead(s)</div>", unsafe_allow_html=True)
            for lead in leads_c:
                status_css = {"Novo":"border-left:3px solid #2e7d32","Em Negociação":"border-left:3px solid #FA8200","Qualificado":"border-left:3px solid #1565C0","Perdido":"border-left:3px solid #c62828","Aguardando":"border-left:3px solid #e8e8e8"}.get(lead.get("status_lead",""),"border-left:3px solid #e8e8e8")
                vol_tag  = f'<span class="crm-tag orange">{lead["volume"]}</span>' if lead.get("volume") else ""
                inc_tag  = f'<span class="crm-tag blue">{lead["incoterm"]}</span>'  if lead.get("incoterm") else ""
                comm_tag = f'<span class="crm-tag green">{lead["commodity"]}</span>' if lead.get("commodity") else ""
                destino  = " → ".join(filter(None, [lead.get("pais_destino",""), lead.get("porto_destino","")]))
                obs      = f'<div class="crm-obs">{lead["observacoes"][:180]}{"…" if len(lead.get("observacoes",""))>180 else ""}</div>' if lead.get("observacoes") else ""
                created  = (lead.get("created_at","") or "")[:16].replace("T"," ")
                st.markdown(f"""
<div class="crm-card" style="{status_css}">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px">
    <div style="flex:1;min-width:0">
      <div class="crm-title">{lead.get("empresa") or lead.get("nome") or "—"}</div>
      <div class="crm-sub">{lead.get("nome","") or ""}{" · " if lead.get("nome") and destino else ""}{destino}</div>
      <div class="crm-tags">{comm_tag}{vol_tag}{inc_tag}</div>{obs}
    </div>
    <div style="text-align:right;flex-shrink:0">
      {_status_badge(lead.get("status_lead","Novo"))}
      <div style="font-size:9px;color:#9CA3AF;margin-top:6px;font-family:Montserrat,sans-serif">{created}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

    with tabs[4]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_due_diligence()

    with tabs[5]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_scripts()

    with tabs[6]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_mapa_filtro()

    with tabs[7]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_lousa()

    with tabs[8]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_socios()

    with tabs[9]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_docs_ref()

    with tabs[10]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_anonimizador()

    with tabs[11]:
        st.markdown(_pad, unsafe_allow_html=True)
        _tab_whatsapp()
