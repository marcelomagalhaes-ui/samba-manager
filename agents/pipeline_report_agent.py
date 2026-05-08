"""
agents/pipeline_report_agent.py
================================
Agente de Relatório de Pipeline — gera e envia email semanal com o andamento
dos deals agrupados por grupo WhatsApp (coluna D da planilha "todos andamento").

Agenda (Celery Beat):
  - Sexta-feira 16:00 BRT
  - Domingo 21:00 BRT

Remetente:    agente@sambaexport.com.br
Destinatários: todos os sócios diretores (Leonardo, Nivio, Marcelo)

Uso direto (sem Celery):
    python -X utf8 agents/pipeline_report_agent.py
"""
from __future__ import annotations

import datetime
import logging
import os
import sys
from collections import defaultdict
from typing import Any

# Garante que a raiz do projeto esteja no path (necessário ao rodar direto)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from googleapiclient.discovery import build

from services.email_service import EmailService, SAMBA_AGENT_EMAIL
from services.google_drive import drive_manager
from services.internal_notify import ASSIGNEE_EMAILS

logger = logging.getLogger("PipelineReportAgent")

# ─── Planilha ────────────────────────────────────────────────────────────────
SPREADSHEET_ID = os.getenv(
    "SAMBA_SPREADSHEET_ID",
    "1ToNZxYYi0dPQkQ0bRE8W3DWXJxkzEVS4vLQyrj2VP9U",
)
SHEET_TAB = "todos andamento"

# Índices das colunas (0-based, A=0)
COL_JOB          = 0   # A  — identificador da oferta
COL_DATA         = 1   # B  — data de entrada
COL_OFERTA       = 2   # C  — oferta/pedido
COL_GRUPO        = 3   # D  — grupo WhatsApp (chave de agrupamento)
COL_SOLICITANTE  = 4   # E  — solicitante
COL_STATUS       = 5   # F  — status comercial
COL_PRODUTO      = 6   # G  — produto/commodity
COL_COMPRADOR    = 7   # H  — comprador
COL_FORNECEDOR   = 8   # I  — fornecedor
COL_VIZ          = 9   # J  — viz rápida
COL_ESPECIFICACAO = 11  # L  — especificação
COL_SITUACAO     = 12  # M  — situação
COL_ACAO         = 13  # N  — ação
COL_STATUS_AUTO  = 14  # O  — status automação

# Status de automação que excluem o deal do relatório
SKIP_AUTO_STATUSES = {"REJECTED", "SKIPPED"}

# Valores que indicam uma linha de cabeçalho/estrutural (ignorar)
HEADER_SENTINEL_GRUPO  = {"grupo", ""}
HEADER_SENTINEL_STATUS = {"status", ""}

# Campos obrigatórios para considerar o deal "completo"
CAMPOS_OBRIGATORIOS = {
    COL_PRODUTO:    "Produto",
    COL_COMPRADOR:  "Comprador",
    COL_FORNECEDOR: "Fornecedor",
    COL_SITUACAO:   "Situação",
}

# ─── Paleta oficial Samba Export ─────────────────────────────────────────────
# Manual de Marca 2026 — R:250 G:130 B:0
STATUS_COLORS: dict[str, str] = {
    "EM DEAL":                 "#329632",
    "PENDENTE COMPRADOR":      "#fa8200",
    "PENDENTE VENDEDOR":       "#326496",
    "PENDENTE SAMBA":          "#fa3232",
    "PEND. COMPRADOR":         "#fa8200",
    "PEND. VENDEDOR":          "#326496",
    "PEND. SAMBA":             "#fa3232",
    "PENDENTE COMPRADOR ":     "#fa8200",  # trailing space variant
    "PENDENTE VENDEDOR ":      "#326496",
    "REUNIÃO AGENDADA":        "#64c8fa",
    "AGUARDANDO REUNIÃO":      "#64c8fa",
    "LEAD CAPTURADO":          "#64c8fa",
    "CONCLUIDO":               "#329632",
    "CONCLUÍDO":               "#329632",
    "CANCELADO":               "#9a9aa0",
    "PERDIDO":                 "#fa3232",
    "PROCURAR COMPRADOR":      "#fa8200",
    "APRESENTAÇÃO":            "#64c8fa",
}
DEFAULT_STATUS_COLOR = "#9a9aa0"


def _status_color(status: str) -> str:
    return STATUS_COLORS.get(status.strip().upper(), DEFAULT_STATUS_COLOR)


# ─── Helpers de HTML ─────────────────────────────────────────────────────────

def _safe(val: Any, max_len: int = 60) -> str:
    s = str(val or "—").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return s[:max_len] + ("…" if len(s) > max_len else "")


def _status_badge(status: str) -> str:
    color = _status_color(status)
    return (
        f'<span style="display:inline-block;background:{color}22;color:{color};'
        f'border:1px solid {color};border-radius:10px;padding:2px 10px;'
        f'font-size:10px;font-weight:700;letter-spacing:0.8px;'
        f'text-transform:uppercase;white-space:nowrap;">{_safe(status, 25)}</span>'
    )


def _incomplete_badge() -> str:
    return (
        f'<span style="display:inline-block;background:#fa323222;color:#fa3232;'
        f'border:1px solid #fa3232;border-radius:10px;padding:2px 8px;'
        f'font-size:9px;font-weight:700;letter-spacing:0.5px;margin-left:4px;">'
        f'⚠ INCOMPLETO</span>'
    )


def _missing_fields(row: list[str]) -> list[str]:
    """Retorna lista de nomes dos campos obrigatórios vazios nesta linha."""
    faltando = []
    for col_idx, nome in CAMPOS_OBRIGATORIOS.items():
        val = row[col_idx].strip() if len(row) > col_idx else ""
        if not val:
            faltando.append(nome)
    return faltando


def _deal_row(row: list[str]) -> str:
    """Linha de deal dentro de um grupo."""
    def col(i: int, default: str = "") -> str:
        return row[i].strip() if len(row) > i else default

    job        = col(COL_JOB) or "—"
    data       = col(COL_DATA) or "—"
    produto    = col(COL_PRODUTO) or "—"
    comprador  = col(COL_COMPRADOR) or "—"
    fornecedor = col(COL_FORNECEDOR) or "—"
    status     = col(COL_STATUS) or "—"
    situacao   = col(COL_SITUACAO) or "—"
    acao       = col(COL_ACAO)

    sc     = _status_color(status)
    falta  = _missing_fields(row)
    incompleto = len(falta) > 0

    # Rótulo de job — pode ser vazio, usa grupo+linha como fallback
    job_display = job if job != "—" else f'<em style="color:#9a9aa0;">sem JOB</em>'

    # Indicador de campos faltantes inline
    campos_faltando_html = ""
    if incompleto:
        badges = " ".join(
            f'<span style="color:#fa8200;font-size:9px;font-weight:600;">{f}</span>'
            for f in falta
        )
        campos_faltando_html = (
            f'<div style="margin-top:3px;color:#fa8200;font-size:9px;">'
            f'⚠ faltando: {badges}</div>'
        )

    tr_bg      = 'background:#1a0a00;' if incompleto else ""
    badge_incl = _incomplete_badge() if incompleto else ""
    acao_html  = (
        '<br><span style="color:#64c8fa;font-size:10px;">' + _safe(acao, 50) + '</span>'
        if acao else ""
    )

    return (
        f'<tr style="{tr_bg}">'
        f'<td style="padding:8px 10px;border-bottom:1px solid rgba(245,245,247,0.05);'
        f'border-left:3px solid {sc};color:#f5f5f7;font-size:12px;font-weight:600;">'
        f'{job_display}'
        f'<div style="color:#9a9aa0;font-size:10px;font-weight:400;margin-top:2px;">{_safe(data, 12)}</div>'
        f'</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid rgba(245,245,247,0.05);'
        f'color:#c0c0c8;font-size:12px;">'
        f'{_safe(produto, 35)}'
        f'<div style="color:#9a9aa0;font-size:10px;margin-top:2px;">'
        f'C: {_safe(comprador, 22)} · F: {_safe(fornecedor, 22)}</div>'
        f'{campos_faltando_html}'
        f'</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid rgba(245,245,247,0.05);">'
        f'{_status_badge(status)}{badge_incl}'
        f'</td>'
        f'<td style="padding:8px 10px;border-bottom:1px solid rgba(245,245,247,0.05);'
        f'color:#9a9aa0;font-size:11px;max-width:180px;">'
        f'{_safe(situacao, 60)}{acao_html}'
        f'</td>'
        f'</tr>'
    )


def _group_block(grupo: str, rows: list[list[str]], color_idx: int) -> str:
    """Bloco completo de um grupo com tabela de deals."""
    accent_colors = ["#fa8200", "#64c8fa", "#329632", "#fa3232", "#326496"]
    accent = accent_colors[color_idx % len(accent_colors)]
    n = len(rows)

    # Contagem de status no grupo
    status_counts: dict[str, int] = defaultdict(int)
    n_incompletos = 0
    for r in rows:
        s = r[COL_STATUS].strip() if len(r) > COL_STATUS else "—"
        status_counts[s or "—"] += 1
        if _missing_fields(r):
            n_incompletos += 1

    kpi_pills = "".join(
        f'<span style="margin-right:6px;display:inline-block;'
        f'background:{_status_color(s)}22;color:{_status_color(s)};'
        f'border:1px solid {_status_color(s)};'
        f'border-radius:8px;padding:1px 8px;font-size:10px;font-weight:700;">'
        f'{cnt} {s}</span>'
        for s, cnt in sorted(status_counts.items(), key=lambda x: -x[1])
    )

    incompleto_pill = ""
    if n_incompletos > 0:
        incompleto_pill = (
            f'<span style="display:inline-block;margin-left:4px;'
            f'background:#fa323222;color:#fa3232;border:1px solid #fa3232;'
            f'border-radius:8px;padding:1px 8px;font-size:10px;font-weight:700;">'
            f'⚠ {n_incompletos} incompleto{"s" if n_incompletos != 1 else ""}</span>'
        )

    deal_rows_html = "".join(_deal_row(r) for r in rows)

    return (
        f'<div style="margin-bottom:24px;background:#141418;border-radius:8px;'
        f'overflow:hidden;border:1px solid rgba(250,130,0,0.12);">'

        # cabeçalho do grupo
        f'<div style="background:#09090b;padding:12px 16px;'
        f'border-left:4px solid {accent};border-bottom:1px solid rgba(250,130,0,0.12);">'
        f'<div style="color:{accent};font-size:13px;font-weight:700;'
        f'letter-spacing:0.5px;">{_safe(grupo, 60)}</div>'
        f'<div style="margin-top:6px;">{kpi_pills}{incompleto_pill}</div>'
        f'<div style="color:#9a9aa0;font-size:10px;margin-top:4px;">'
        f'{n} deal{"s" if n != 1 else ""} em andamento</div>'
        f'</div>'

        # tabela de deals
        f'<table width="100%" cellpadding="0" cellspacing="0" '
        f'style="border-collapse:collapse;">'
        f'<thead>'
        f'<tr style="background:#0d0d10;">'
        f'<th style="padding:6px 10px;color:#9a9aa0;font-size:10px;font-weight:700;'
        f'letter-spacing:1px;text-align:left;text-transform:uppercase;min-width:90px;">JOB / DATA</th>'
        f'<th style="padding:6px 10px;color:#9a9aa0;font-size:10px;font-weight:700;'
        f'letter-spacing:1px;text-align:left;text-transform:uppercase;">PRODUTO / PARTES</th>'
        f'<th style="padding:6px 10px;color:#9a9aa0;font-size:10px;font-weight:700;'
        f'letter-spacing:1px;text-align:left;text-transform:uppercase;white-space:nowrap;">STATUS</th>'
        f'<th style="padding:6px 10px;color:#9a9aa0;font-size:10px;font-weight:700;'
        f'letter-spacing:1px;text-align:left;text-transform:uppercase;">SITUAÇÃO / AÇÃO</th>'
        f'</tr>'
        f'</thead>'
        f'<tbody>{deal_rows_html}</tbody>'
        f'</table>'
        f'</div>'
    )


def _kpi_bar(grupos: dict[str, list], total_deals: int, total_incompletos: int) -> str:
    all_statuses: list[str] = []
    for rows in grupos.values():
        for r in rows:
            s = r[COL_STATUS].strip() if len(r) > COL_STATUS else ""
            if s:
                all_statuses.append(s.upper())

    em_deal    = sum(1 for s in all_statuses if s == "EM DEAL")
    pend_total = sum(1 for s in all_statuses if "PEND" in s)
    reunioes   = sum(1 for s in all_statuses if "REUNI" in s or "AGUARDANDO" in s)
    concluidos = sum(1 for s in all_statuses if "CONCLU" in s)

    def _kpi_cell(label: str, value: str, color: str) -> str:
        return (
            f'<td align="center" style="padding:12px 6px;">'
            f'<div style="color:{color};font-size:22px;font-weight:700;">{value}</div>'
            f'<div style="color:#9a9aa0;font-size:9px;letter-spacing:1px;'
            f'text-transform:uppercase;margin-top:2px;">{label}</div>'
            f'</td>'
        )

    return (
        f'<div style="margin-bottom:24px;background:#0d0d10;border-radius:8px;'
        f'border:1px solid rgba(250,130,0,0.15);overflow:hidden;">'
        f'<table width="100%" cellpadding="0" cellspacing="0"><tr>'
        + _kpi_cell("Total Deals",    str(total_deals),        "#fa8200")
        + _kpi_cell("Grupos Ativos",  str(len(grupos)),        "#64c8fa")
        + _kpi_cell("Em Deal",        str(em_deal),            "#329632")
        + _kpi_cell("Pendências",     str(pend_total),         "#fa3232")
        + _kpi_cell("Reuniões",       str(reunioes),           "#326496")
        + _kpi_cell("Concluídos",     str(concluidos),         "#9a9aa0")
        + _kpi_cell("⚠ Incompletos",  str(total_incompletos),  "#fa3232" if total_incompletos else "#9a9aa0")
        + f'</tr></table>'
        f'</div>'
    )


def _incompletos_alert(incompletos: list[tuple[str, list[str], list[str]]]) -> str:
    """
    Bloco de alerta de deals incompletos — aparece logo após o KPI bar.
    incompletos: lista de (grupo, row, campos_faltando)
    """
    if not incompletos:
        return ""

    items_html = ""
    for grupo, row, falta in incompletos[:25]:  # max 25 no alert
        job    = row[COL_JOB].strip()   if len(row) > COL_JOB    else ""
        status = row[COL_STATUS].strip() if len(row) > COL_STATUS else "—"
        sc = _status_color(status)
        label = job or f"(linha sem JOB — grupo {grupo})"
        falta_str = ", ".join(falta)
        items_html += (
            f'<tr>'
            f'<td style="padding:6px 10px;border-bottom:1px solid rgba(245,245,247,0.05);'
            f'color:#f5f5f7;font-size:11px;font-weight:600;border-left:2px solid {sc};">'
            f'{_safe(label, 25)}'
            f'<div style="color:#9a9aa0;font-size:10px;">{_safe(grupo, 20)}</div>'
            f'</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid rgba(245,245,247,0.05);">'
            f'{_status_badge(status)}'
            f'</td>'
            f'<td style="padding:6px 10px;border-bottom:1px solid rgba(245,245,247,0.05);'
            f'color:#fa8200;font-size:10px;font-weight:600;">'
            f'{_safe(falta_str, 50)}'
            f'</td>'
            f'</tr>'
        )

    return (
        f'<div style="margin-bottom:24px;background:#1a0a00;border-radius:8px;'
        f'overflow:hidden;border:1px solid #fa323240;">'
        f'<div style="background:#2a0a00;padding:12px 16px;'
        f'border-left:4px solid #fa3232;border-bottom:1px solid #fa323430;">'
        f'<div style="color:#fa3232;font-size:12px;font-weight:700;letter-spacing:0.5px;">'
        f'⚠️ {len(incompletos)} DEAL{"S" if len(incompletos) != 1 else ""} COM DADOS INCOMPLETOS NA PLANILHA</div>'
        f'<div style="color:#c0c0c8;font-size:11px;margin-top:4px;">'
        f'Os campos abaixo estão em branco. O Extractor Agent não consegue processar deals sem essas informações — '
        f'preencha a planilha ou peça os dados ao parceiro.</div>'
        f'</div>'
        f'<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">'
        f'<thead><tr style="background:#200a00;">'
        f'<th style="padding:6px 10px;color:#9a9aa0;font-size:9px;font-weight:700;'
        f'letter-spacing:1px;text-align:left;text-transform:uppercase;">JOB / GRUPO</th>'
        f'<th style="padding:6px 10px;color:#9a9aa0;font-size:9px;font-weight:700;'
        f'letter-spacing:1px;text-align:left;text-transform:uppercase;">STATUS</th>'
        f'<th style="padding:6px 10px;color:#9a9aa0;font-size:9px;font-weight:700;'
        f'letter-spacing:1px;text-align:left;text-transform:uppercase;">CAMPOS FALTANDO</th>'
        f'</tr></thead>'
        f'<tbody>{items_html}</tbody>'
        f'</table>'
        f'</div>'
    )


def _executive_summary(
    grupos: dict[str, list],
    total_deals: int,
    total_incompletos: int,
    dt_str: str,
) -> str:
    all_statuses = []
    for rows in grupos.values():
        for r in rows:
            s = r[COL_STATUS].strip().upper() if len(r) > COL_STATUS else ""
            if s:
                all_statuses.append(s)

    em_deal    = sum(1 for s in all_statuses if s == "EM DEAL")
    pendencias = sum(1 for s in all_statuses if "PEND" in s)

    if total_incompletos > 0:
        saude = (
            f'🔴 {total_incompletos} deal{"s" if total_incompletos != 1 else ""}'
            f' com dados incompletos — ação necessária.'
        )
    elif pendencias > 0:
        saude = "🟡 Há pendências aguardando resolução."
    else:
        saude = "🟢 Pipeline saudável — sem bloqueios críticos."

    incompleto_frag = (
        f', <strong style="color:#fa3232;">'
        f'{total_incompletos} com dados incompletos</strong>'
        if total_incompletos else ""
    )

    return (
        f'<div style="margin-bottom:20px;padding:16px 20px;'
        f'background:linear-gradient(135deg,#1a0e00 0%,#0d0d10 100%);'
        f'border-radius:8px;border-left:4px solid #fa8200;">'
        f'<div style="color:#fa8200;font-size:11px;font-weight:700;'
        f'letter-spacing:1.5px;text-transform:uppercase;margin-bottom:10px;">'
        f'📊 Sumário Executivo — {dt_str}</div>'
        f'<p style="color:#f5f5f7;font-size:13px;line-height:1.7;margin:0 0 8px;">'
        f'Pipeline Samba Export: <strong style="color:#fa8200;">{total_deals} deals ativos</strong> '
        f'em <strong style="color:#64c8fa;">{len(grupos)} grupos</strong>. '
        f'<strong style="color:#329632;">{em_deal} em fase EM DEAL</strong>, '
        f'<strong style="color:#fa3232;">{pendencias} pendências</strong> aguardando resolução'
        f'{incompleto_frag}.'
        f'</p>'
        f'<p style="color:#c0c0c8;font-size:12px;margin:0;">{saude}</p>'
        f'</div>'
    )


def _legend_block() -> str:
    items = [
        ("EM DEAL",          "#329632"),
        ("PEND. COMPRADOR",  "#fa8200"),
        ("PEND. VENDEDOR",   "#326496"),
        ("PEND. SAMBA",      "#fa3232"),
        ("REUNIÃO AGENDADA", "#64c8fa"),
        ("CONCLUÍDO",        "#329632"),
        ("CANCELADO",        "#9a9aa0"),
    ]
    pills = "".join(
        f'<span style="display:inline-block;margin:3px 4px;background:{c}22;color:{c};'
        f'border:1px solid {c};border-radius:8px;padding:2px 10px;'
        f'font-size:10px;font-weight:700;">{lbl}</span>'
        for lbl, c in items
    )
    return (
        f'<div style="margin-top:20px;padding:12px 16px;background:#0d0d10;'
        f'border-radius:6px;border-top:2px solid rgba(250,130,0,0.20);">'
        f'<div style="color:#9a9aa0;font-size:10px;letter-spacing:1px;'
        f'text-transform:uppercase;margin-bottom:8px;">Legenda de Status</div>'
        f'{pills}'
        f'</div>'
    )


# ─── Classe principal ─────────────────────────────────────────────────────────

class PipelineReportAgent:
    """
    Lê a planilha Google Sheets, filtra e agrupa por GRUPO (coluna D),
    monta HTML com paleta oficial Samba Export e envia para todos os diretores.

    Regras de filtragem:
      - Ignora qualquer linha onde GRUPO está vazio OU é literal "GRUPO" (header)
      - Ignora qualquer linha onde STATUS_AUTOMACAO == REJECTED ou SKIPPED
      - Inclui TUDO mais, incluindo PENDING_IA e linhas sem JOB preenchido
    """

    def __init__(self) -> None:
        self.email_svc = EmailService()
        self._sheets_svc = None

    def _get_sheets(self):
        if self._sheets_svc is None:
            if not drive_manager.creds or not drive_manager.creds.valid:
                raise RuntimeError("PipelineReportAgent: credenciais Drive inválidas.")
            self._sheets_svc = build(
                "sheets", "v4", credentials=drive_manager.creds, cache_discovery=False
            )
        return self._sheets_svc

    # ─── Leitura da planilha ──────────────────────────────────────────────────

    def _read_sheet(self) -> list[list[str]]:
        svc = self._get_sheets()
        range_name = f"'{SHEET_TAB}'!A:O"
        result = (
            svc.spreadsheets()
            .values()
            .get(spreadsheetId=SPREADSHEET_ID, range=range_name)
            .execute()
        )
        return result.get("values", [])

    def _active_deals(self, all_rows: list[list[str]]) -> list[list[str]]:
        """
        Filtra deals ativos de todas as linhas da planilha.

        Critério de EXCLUSÃO (linha é ignorada):
          1. GRUPO está vazio → linha estrutural/vazia
          2. GRUPO == "GRUPO" (literal) → linha de cabeçalho
          3. STATUS (col F) == "STATUS" (literal) → linha de cabeçalho
          4. STATUS_AUTOMACAO (col O) == "REJECTED" ou "SKIPPED"

        Critério de INCLUSÃO: tudo mais, incluindo:
          - Linhas com JOB vazio (campo preenchido mais tarde pelo operador)
          - Linhas com STATUS_AUTOMACAO == "PENDING_IA" (aguardando extractor)
          - Linhas com STATUS_AUTOMACAO vazio (não processadas ainda)
        """
        active = []
        for row in all_rows:
            if not row:
                continue

            grupo = row[COL_GRUPO].strip() if len(row) > COL_GRUPO else ""
            status = row[COL_STATUS].strip() if len(row) > COL_STATUS else ""
            auto_status = row[COL_STATUS_AUTO].strip().upper() if len(row) > COL_STATUS_AUTO else ""

            # Exclui linhas estruturais/cabeçalho
            if not grupo or grupo.lower() == "grupo":
                continue
            if status.lower() == "status":
                continue

            # Exclui apenas REJECTED e SKIPPED explícitos
            if auto_status in SKIP_AUTO_STATUSES:
                continue

            active.append(row)

        return active

    def _group_by_grupo(
        self, rows: list[list[str]]
    ) -> dict[str, list[list[str]]]:
        grupos: dict[str, list] = defaultdict(list)
        for row in rows:
            grupo = row[COL_GRUPO].strip() if len(row) > COL_GRUPO else "SEM GRUPO"
            grupos[grupo].append(row)
        # Ordena por número de deals decrescente (grupos maiores primeiro)
        return dict(sorted(grupos.items(), key=lambda x: -len(x[1])))

    def _collect_incompletos(
        self, grupos: dict[str, list[list[str]]]
    ) -> list[tuple[str, list[str], list[str]]]:
        """Retorna lista de (grupo, row, campos_faltando) para deals incompletos."""
        result = []
        for grupo, rows in grupos.items():
            for row in rows:
                falta = _missing_fields(row)
                if falta:
                    result.append((grupo, row, falta))
        return result

    # ─── Geração do HTML ──────────────────────────────────────────────────────

    def build_report_html(self) -> tuple[str, dict]:
        all_rows   = self._read_sheet()
        active     = self._active_deals(all_rows)
        grupos     = self._group_by_grupo(active)
        incompletos = self._collect_incompletos(grupos)

        total_deals        = len(active)
        total_grupos       = len(grupos)
        total_incompletos  = len(incompletos)

        now     = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-3)))
        dt_str  = now.strftime("%d/%m/%Y %H:%M BRT")
        weekday_pt = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
        day_name   = weekday_pt[now.weekday()]

        # ── Corpo do email ───────────────────────────────────────────────────
        body = _kpi_bar(grupos, total_deals, total_incompletos)
        body += _executive_summary(grupos, total_deals, total_incompletos, dt_str)

        # Alerta de incompletos logo após sumário (se houver)
        body += _incompletos_alert(incompletos)

        # Blocos por grupo (ordenados por tamanho)
        for idx, (grupo, rows) in enumerate(grupos.items()):
            body += _group_block(grupo, rows, idx)

        body += _legend_block()

        # Rodapé com agenda
        body += (
            f'<div style="margin-top:16px;padding:10px 14px;background:#09090b;'
            f'border-radius:6px;border:1px solid rgba(250,130,0,0.10);">'
            f'<span style="color:#9a9aa0;font-size:10px;">'
            f'📅 Relatório automático: toda <strong style="color:#fa8200;">Sexta-feira às 16h</strong> '
            f'e <strong style="color:#fa8200;">Domingo às 21h</strong> (horário de Brasília) · '
            f'<a href="https://sambaexport.com.br" style="color:#fa8200;text-decoration:none;">'
            f'sambaexport.com.br</a></span>'
            f'</div>'
        )

        html = EmailService.build_html(
            title=f"📊 Relatório de Pipeline — {day_name}, {dt_str}",
            subtitle=(
                f"{total_deals} deals ativos · {total_grupos} grupos"
                + (f" · ⚠ {total_incompletos} incompleto{'s' if total_incompletos != 1 else ''}" if total_incompletos else "")
                + " · Samba Agent"
            ),
            body_html=body,
            icon="📊",
        )

        meta = {
            "total_deals":       total_deals,
            "total_grupos":      total_grupos,
            "total_incompletos": total_incompletos,
            "groups":            {g: len(r) for g, r in grupos.items()},
            "generated_at":      dt_str,
        }
        return html, meta

    # ─── Envio ────────────────────────────────────────────────────────────────

    def run(self) -> dict:
        logger.info("PipelineReportAgent: iniciando leitura da planilha...")

        try:
            html, meta = self.build_report_html()
        except Exception as exc:
            logger.error("PipelineReportAgent: erro ao gerar relatório — %s", exc)
            return {"status": "error", "reason": str(exc)}

        destinatarios = list(ASSIGNEE_EMAILS.values())

        now     = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-3)))
        dt_subj = now.strftime("%d/%m/%Y")
        weekday_pt = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
        day_pt     = weekday_pt[now.weekday()]

        incompleto_tag = (
            f" | ⚠ {meta['total_incompletos']} incompleto{'s' if meta['total_incompletos'] != 1 else ''}"
            if meta['total_incompletos'] else ""
        )

        ok = self.email_svc.send_html(
            to=destinatarios,
            subject=(
                f"[Samba] 📊 Pipeline Report — {day_pt} {dt_subj}"
                f" | {meta['total_deals']} deals · {meta['total_grupos']} grupos{incompleto_tag}"
            ),
            html_body=html,
            cc=[SAMBA_AGENT_EMAIL],
        )

        result = {
            "status":            "ok" if ok else "error_send",
            "email_sent":        ok,
            "destinatarios":     destinatarios,
            **meta,
        }
        logger.info("PipelineReportAgent resultado=%s", result)
        return result


# ─── Entry-point direto ───────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    print("=" * 70)
    print("PIPELINE REPORT AGENT — Samba Export")
    print("=" * 70)

    agent  = PipelineReportAgent()
    result = agent.run()

    print("\nResultado:")
    for k, v in result.items():
        print(f"  {k}: {v}")

    sys.exit(0 if result.get("email_sent") else 1)
