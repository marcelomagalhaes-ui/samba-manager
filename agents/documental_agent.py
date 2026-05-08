"""
agents/documental_agent.py
===========================
DocumentalAgent — Motor de Conformidade Documental da Samba Export.
Sprint C — Auditoria ICC/UCP 600 com ground truth embedded.

Responsabilidades:
  * Extrair texto de PDFs e DOCXs (via pdfplumber / python-docx)
  * Auditar documentos contra clausulas obrigatorias ICC/GAFTA
  * Detectar violacoes UCP 600 citando artigos especificos
  * Verificar specs tecnicas de commodities vs. commodities_specs.json
  * Verificar pronome corporativo: 'its' (nunca 'his'/'her') — regra ICC
  * Retornar ComplianceReport com status VERDE/AMARELO/VERMELHO
  * Persistir resultado na tabela document_compliance

Uso:
    agent = DocumentalAgent()
    report = agent.auditar_documento("doc.pdf", "FCO", "soja_gmo")

CLI:
    python agents/documental_agent.py --file doc.pdf --type FCO --commodity soja_gmo
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.document_schemas import (
    ComplianceReport,
    ComplianceStatus,
    DocumentType,
    MissingClause,
    SpecDivergence,
)
from models.database import get_session
import models.database as db_mod


# ─────────────────────────────────────────────────────────────────────────────
# Ground truth — specs de commodities
# ─────────────────────────────────────────────────────────────────────────────

SPECS_PATH = ROOT / "data" / "knowledge" / "commodities_specs.json"

_specs_cache: Optional[dict] = None


def _load_specs() -> dict:
    global _specs_cache
    if _specs_cache is None:
        if SPECS_PATH.exists():
            with open(SPECS_PATH, encoding="utf-8") as f:
                _specs_cache = json.load(f)
        else:
            _specs_cache = {}
    return _specs_cache


# ─────────────────────────────────────────────────────────────────────────────
# Regras de clausulas obrigatorias por tipo de documento
# ─────────────────────────────────────────────────────────────────────────────

MANDATORY_CLAUSES: dict[str, list[dict]] = {
    "LOI": [
        {"name": "Identificacao do Comprador", "keywords": ["buyer", "comprador", "purchaser"],
         "rule": "ICC LOI Standard", "severity": "CRITICA"},
        {"name": "Commodity e Volume", "keywords": ["commodity", "quantity", "metric ton", "mt"],
         "rule": "ICC LOI Standard", "severity": "CRITICA"},
        {"name": "Incoterm", "keywords": ["cif", "fob", "cfr", "incoterm"],
         "rule": "Incoterms 2020 ICC Pub. 723", "severity": "CRITICA"},
        {"name": "Instrumento de Pagamento DLC", "keywords": ["letter of credit", "dlc", "ucp 600", "l/c"],
         "rule": "UCP 600 ICC Pub. 600", "severity": "CRITICA"},
        {"name": "Banco Top 50/100", "keywords": ["top 50", "top 100", "first class bank", "prime bank"],
         "rule": "UCP 600 — banco emitente qualificado", "severity": "IMPORTANTE"},
        {"name": "Autorizacao Soft Probe / POF", "keywords": ["soft probe", "proof of funds", "pof", "non-debit"],
         "rule": "Pratica internacional — verificacao de fundos", "severity": "IMPORTANTE"},
        {"name": "Clausula de Confidencialidade / NCNDA", "keywords": ["confidential", "ncnda", "non-disclosure"],
         "rule": "ICC NCNDA Standard", "severity": "IMPORTANTE"},
        {"name": "Assinatura e Carimbo", "keywords": ["signature", "authorized", "signed", "seal", "company stamp"],
         "rule": "Validade juridica do documento", "severity": "CRITICA"},
    ],
    "ICPO": [
        {"name": "Identificacao do Comprador", "keywords": ["buyer", "purchaser", "company name", "registration"],
         "rule": "ICC ICPO Standard", "severity": "CRITICA"},
        {"name": "Board Resolution / POA", "keywords": ["board resolution", "power of attorney", "poa", "authorized"],
         "rule": "ICC ICPO — autorizacao corporativa", "severity": "CRITICA"},
        {"name": "Commodity e Especificacoes", "keywords": ["commodity", "specification", "grade", "quality"],
         "rule": "ICC ICPO Standard", "severity": "CRITICA"},
        {"name": "Volume e Preco", "keywords": ["metric ton", "quantity", "price", "usd"],
         "rule": "ICC ICPO Standard", "severity": "CRITICA"},
        {"name": "Incoterm", "keywords": ["cif", "fob", "cfr", "incoterm"],
         "rule": "Incoterms 2020 ICC Pub. 723", "severity": "CRITICA"},
        {"name": "DLC Irrevogavel UCP 600", "keywords": ["irrevocable", "letter of credit", "ucp 600", "transferable", "divisible"],
         "rule": "UCP 600 ICC Pub. 600 — Art. 2", "severity": "CRITICA"},
        {"name": "Banco Top 50/100", "keywords": ["top 50", "top 100", "first class", "prime bank"],
         "rule": "UCP 600 — banco emitente qualificado", "severity": "CRITICA"},
        {"name": "Coordenadas Bancarias do Comprador", "keywords": ["swift", "iban", "account number", "bank name"],
         "rule": "ICC ICPO Standard — identificacao bancaria", "severity": "IMPORTANTE"},
        {"name": "Autorizacao Soft Probe / POF", "keywords": ["soft probe", "proof of funds", "non-debit", "pof"],
         "rule": "Pratica internacional — verificacao de fundos", "severity": "CRITICA"},
        {"name": "Performance Bond 2%", "keywords": ["performance bond", "2%", "two percent"],
         "rule": "Samba Export Standard — protecao contratual", "severity": "IMPORTANTE"},
        {"name": "Irrevogabilidade do ICPO", "keywords": ["irrevocable", "irrevogavel", "binding"],
         "rule": "ICC ICPO Standard", "severity": "CRITICA"},
        {"name": "Validade", "keywords": ["valid", "validity", "expire", "expiration"],
         "rule": "ICC ICPO Standard", "severity": "IMPORTANTE"},
        {"name": "Assinatura e Carimbo", "keywords": ["signature", "authorized", "signed", "seal"],
         "rule": "Validade juridica do documento", "severity": "CRITICA"},
    ],
    "FCO": [
        {"name": "Identificacao do Vendedor", "keywords": ["seller", "vendor", "company name", "registered"],
         "rule": "ICC FCO Standard", "severity": "CRITICA"},
        {"name": "Identificacao do Comprador", "keywords": ["buyer", "purchaser"],
         "rule": "ICC FCO Standard", "severity": "CRITICA"},
        {"name": "Commodity e Grade", "keywords": ["commodity", "grade", "specification"],
         "rule": "ICC FCO Standard", "severity": "CRITICA"},
        {"name": "Volume e Schedule", "keywords": ["metric ton", "mt per month", "delivery schedule", "quantity"],
         "rule": "ICC FCO Standard", "severity": "CRITICA"},
        {"name": "Preco CIF/FOB", "keywords": ["usd", "per metric ton", "price"],
         "rule": "ICC FCO Standard", "severity": "CRITICA"},
        {"name": "Incoterm", "keywords": ["cif", "fob", "cfr", "incoterm"],
         "rule": "Incoterms 2020 ICC Pub. 723", "severity": "CRITICA"},
        {"name": "DLC Irrevogavel Transferivel Divisivel", "keywords": ["irrevocable", "transferable", "divisible", "letter of credit", "ucp 600"],
         "rule": "UCP 600 ICC Pub. 600 — Art. 2", "severity": "CRITICA"},
        {"name": "Banco Top 50/100", "keywords": ["top 50", "top 100", "first class bank"],
         "rule": "UCP 600 — banco emitente qualificado", "severity": "CRITICA"},
        {"name": "Prazo Emissao DLC (7-10 dias)", "keywords": ["7", "ten", "banking days", "dias uteis"],
         "rule": "UCP 600 — prazo padrao SPA>DLC", "severity": "IMPORTANTE"},
        {"name": "Especificacoes Tecnicas da Commodity", "keywords": ["protein", "moisture", "oil content", "foreign matter", "icumsa", "humidity"],
         "rule": "GAFTA 100 / ANEC / FOSFA — specs obrigatorias", "severity": "CRITICA"},
        {"name": "Procedimento de Transacao", "keywords": ["loi", "fco", "spa", "pof", "dlc", "shipment", "procedure"],
         "rule": "Samba Export Standard — sequencia contratual", "severity": "IMPORTANTE"},
        {"name": "Performance Bond 2%", "keywords": ["performance bond", "2%", "two percent"],
         "rule": "Samba Export Standard", "severity": "IMPORTANTE"},
        {"name": "Inspecao por Entidade Neutra", "keywords": ["sgs", "ccic", "bureau veritas", "intertek", "inspection"],
         "rule": "GAFTA 100 — inspecao neutra", "severity": "CRITICA"},
        {"name": "Seguro 110% em Nome do Comprador", "keywords": ["110%", "insurance", "buyer", "institute cargo"],
         "rule": "UCP 600 Art. 28 — seguro documentario", "severity": "CRITICA"},
        {"name": "Coordenadas Bancarias do Vendedor", "keywords": ["swift", "iban", "account", "bank"],
         "rule": "ICC FCO Standard — instrucoes de pagamento", "severity": "IMPORTANTE"},
        {"name": "Clausula de Force Majeure", "keywords": ["force majeure", "act of god", "beyond", "control"],
         "rule": "ICC Standard — clausula de caso fortuito", "severity": "IMPORTANTE"},
        {"name": "Clausula de Confidencialidade / NCNDA", "keywords": ["confidential", "ncnda", "private"],
         "rule": "ICC NCNDA Standard", "severity": "IMPORTANTE"},
        {"name": "Referencia IMFPA", "keywords": ["imfpa", "fee protection", "commission"],
         "rule": "Samba Export Standard — protecao de comissoes", "severity": "RECOMENDADA"},
        {"name": "Clausula de Arbitragem / GAFTA", "keywords": ["arbitration", "gafta", "fosfa", "dispute"],
         "rule": "GAFTA Arbitration Rules — resolucao de disputas", "severity": "IMPORTANTE"},
        {"name": "Assinatura e Carimbo", "keywords": ["signature", "authorized", "signed", "seal"],
         "rule": "Validade juridica do documento", "severity": "CRITICA"},
    ],
    "SPA": [
        {"name": "Identificacao Completa das Partes", "keywords": ["seller", "buyer", "registration", "represented by"],
         "rule": "ICC SPA Standard", "severity": "CRITICA"},
        {"name": "Board Resolution das Partes", "keywords": ["board resolution", "power of attorney", "duly authorized"],
         "rule": "ICC SPA — autorizacao corporativa", "severity": "CRITICA"},
        {"name": "Objeto do Contrato", "keywords": ["commodity", "grade", "quality", "subject matter"],
         "rule": "ICC SPA Standard", "severity": "CRITICA"},
        {"name": "Cronograma de Entregas", "keywords": ["delivery schedule", "shipment", "per month", "metric ton"],
         "rule": "ICC SPA Standard", "severity": "CRITICA"},
        {"name": "Preco e Incoterm", "keywords": ["price", "usd", "incoterm", "cif", "fob"],
         "rule": "ICC SPA + Incoterms 2020", "severity": "CRITICA"},
        {"name": "DLC UCP 600 Irrevogavel", "keywords": ["irrevocable", "letter of credit", "ucp 600", "transferable"],
         "rule": "UCP 600 ICC Pub. 600 — Art. 2", "severity": "CRITICA"},
        {"name": "Banco Top 50/100", "keywords": ["top 50", "top 100", "first class"],
         "rule": "UCP 600 — banco emitente qualificado", "severity": "CRITICA"},
        {"name": "Performance Bond 2%", "keywords": ["performance bond", "2%", "two percent"],
         "rule": "Samba Export Standard", "severity": "CRITICA"},
        {"name": "Penalidade por Nao Entrega", "keywords": ["penalty", "delay", "non-delivery", "0.5%"],
         "rule": "GAFTA 100 — penalidades contratuais", "severity": "CRITICA"},
        {"name": "Inspecao (SGS/CCIC/BV)", "keywords": ["sgs", "ccic", "bureau veritas", "inspection"],
         "rule": "GAFTA 100 — inspecao neutra", "severity": "CRITICA"},
        {"name": "Seguro 110% — ICC A", "keywords": ["110%", "insurance", "institute cargo", "all risk"],
         "rule": "UCP 600 Art. 28 — seguro documentario", "severity": "CRITICA"},
        {"name": "Documentos de Embarque", "keywords": ["bill of lading", "commercial invoice", "packing list", "certificate of origin"],
         "rule": "UCP 600 Art. 18-20 — documentos exigidos", "severity": "CRITICA"},
        {"name": "Clausula de Force Majeure", "keywords": ["force majeure", "act of god", "beyond control"],
         "rule": "ICC Standard", "severity": "IMPORTANTE"},
        {"name": "Governing Law e Arbitragem", "keywords": ["governing law", "arbitration", "gafta", "icc"],
         "rule": "GAFTA Arbitration Rules", "severity": "IMPORTANTE"},
        {"name": "NCNDA Incorporada", "keywords": ["ncnda", "non-circumvention", "incorporated by reference"],
         "rule": "ICC NCNDA Standard", "severity": "IMPORTANTE"},
        {"name": "IMFPA Incorporada", "keywords": ["imfpa", "fee protection", "incorporated by reference"],
         "rule": "Samba Export Standard", "severity": "RECOMENDADA"},
        {"name": "Assinatura de Ambas as Partes", "keywords": ["seller signature", "buyer signature", "signed", "witness"],
         "rule": "Validade juridica — SPA bilateral", "severity": "CRITICA"},
    ],
    "NCNDA": [
        {"name": "Identificacao das Partes", "keywords": ["party", "company", "individual", "registered"],
         "rule": "ICC NCNDA Standard", "severity": "CRITICA"},
        {"name": "Clausula de Nao-Circunvencao", "keywords": ["circumvention", "bypass", "avoid", "direct contact"],
         "rule": "ICC NCNDA Standard", "severity": "CRITICA"},
        {"name": "Definicao de Informacao Confidencial", "keywords": ["confidential", "information", "contact", "pricing"],
         "rule": "ICC NCNDA Standard", "severity": "CRITICA"},
        {"name": "Penalidade por Violacao", "keywords": ["penalty", "liquidated damages", "breach"],
         "rule": "ICC NCNDA Standard", "severity": "IMPORTANTE"},
        {"name": "Vigencia (minimo 2 anos)", "keywords": ["2 years", "two years", "validity", "term"],
         "rule": "ICC NCNDA Standard — duracao minima", "severity": "IMPORTANTE"},
        {"name": "Clausula de Sobrevivencia", "keywords": ["survive", "survival", "termination", "expiration"],
         "rule": "ICC NCNDA Standard", "severity": "IMPORTANTE"},
        {"name": "Governing Law", "keywords": ["governing law", "jurisdiction", "applicable law"],
         "rule": "ICC NCNDA Standard", "severity": "IMPORTANTE"},
        {"name": "Assinatura de Todas as Partes", "keywords": ["signature", "signed", "seal", "authorized"],
         "rule": "Validade juridica", "severity": "CRITICA"},
    ],
    "IMFPA": [
        {"name": "Identificacao das Partes (Vendedor + Comprador + Intermediarios)",
         "keywords": ["seller", "buyer", "intermediary", "mandate"],
         "rule": "Samba Export IMFPA Standard", "severity": "CRITICA"},
        {"name": "Percentual de Comissao de Cada Parte", "keywords": ["%", "percent", "fee", "commission"],
         "rule": "Samba Export IMFPA Standard", "severity": "CRITICA"},
        {"name": "Coordenadas Bancarias dos Beneficiarios", "keywords": ["swift", "iban", "account", "bank"],
         "rule": "Samba Export IMFPA Standard", "severity": "CRITICA"},
        {"name": "Irrevogabilidade", "keywords": ["irrevocable", "irrevogavel"],
         "rule": "Samba Export IMFPA Standard", "severity": "CRITICA"},
        {"name": "Pagamento Simultaneo ao Principal", "keywords": ["simultaneously", "same time", "upon receipt", "concurrent"],
         "rule": "Samba Export IMFPA Standard", "severity": "CRITICA"},
        {"name": "Clausula Anti-Circunvencao de Intermediarios", "keywords": ["circumvention", "bypass", "exclude", "replace"],
         "rule": "Samba Export IMFPA Standard", "severity": "IMPORTANTE"},
        {"name": "Sobrevivencia em Renovacoes do SPA", "keywords": ["renewal", "extension", "survive"],
         "rule": "Samba Export IMFPA Standard", "severity": "IMPORTANTE"},
        {"name": "Assinatura de Todas as Partes", "keywords": ["signature", "signed", "buyer signature", "seller signature"],
         "rule": "Validade juridica", "severity": "CRITICA"},
    ],
}

# Pronomes corporativos incorretos — regra ICC
CORPORATE_PRONOUN_BAD = [r"\bhis\b", r"\bher\b"]
PRONOUN_EXCEPTIONS = re.compile(
    r"(his\s+name|his\s+representative|his\s+capacity|her\s+name|"
    r"his\s+excellency|his\s+honor)",
    re.IGNORECASE
)


# ─────────────────────────────────────────────────────────────────────────────
# Extrator de texto
# ─────────────────────────────────────────────────────────────────────────────

def _extract_text(file_path: str) -> str:
    """Extrai texto de PDF, DOCX ou TXT."""
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext == ".pdf":
        try:
            import pdfplumber
            with pdfplumber.open(file_path) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
            return "\n".join(pages)
        except ImportError:
            raise RuntimeError("pdfplumber nao instalado. Execute: pip install pdfplumber")
        except Exception as exc:
            raise RuntimeError(f"Erro ao extrair PDF: {exc}") from exc

    elif ext in (".docx", ".doc"):
        try:
            import docx
            doc = docx.Document(file_path)
            return "\n".join(p.text for p in doc.paragraphs)
        except ImportError:
            raise RuntimeError("python-docx nao instalado. Execute: pip install python-docx")
        except Exception as exc:
            raise RuntimeError(f"Erro ao extrair DOCX: {exc}") from exc

    elif ext in (".txt", ".md"):
        return path.read_text(encoding="utf-8", errors="ignore")

    else:
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return ""


# ─────────────────────────────────────────────────────────────────────────────
# Verificadores especializados
# ─────────────────────────────────────────────────────────────────────────────

def _check_clauses(text: str, doc_type: str) -> list[MissingClause]:
    """Verifica clausulas obrigatorias por tipo de documento."""
    text_lower = text.lower()
    rules = MANDATORY_CLAUSES.get(doc_type, [])
    missing = []
    for rule in rules:
        found = any(kw.lower() in text_lower for kw in rule["keywords"])
        if not found:
            missing.append(MissingClause(
                clause_name=rule["name"],
                description=(
                    f"Clausula nao detectada. "
                    f"Palavras-chave esperadas: {', '.join(rule['keywords'][:3])}"
                ),
                rule_citation=rule["rule"],
                severity=rule["severity"],
            ))
    return missing


def _check_pronoun(text: str) -> tuple[bool, list[SpecDivergence]]:
    """Verifica uso incorreto de pronomes para entidades corporativas (regra ICC)."""
    divergences = []
    text_lower = text.lower()
    found_bad = False

    for pattern in CORPORATE_PRONOUN_BAD:
        for m in re.finditer(pattern, text_lower):
            ctx_start = max(0, m.start() - 40)
            ctx_end = min(len(text_lower), m.end() + 40)
            context = text_lower[ctx_start:ctx_end]
            if PRONOUN_EXCEPTIONS.search(context):
                continue
            found_bad = True
            break
        if found_bad:
            break

    if found_bad:
        divergences.append(SpecDivergence(
            field="Pronome Corporativo",
            found="'his' ou 'her' detectado para entidade corporativa",
            expected="'its' — pronome neutro obrigatorio para pessoas juridicas",
            rule_citation="ICC/UCP 600 — corporacoes SEMPRE usam 'its', nunca 'his'/'her'",
            severity="IMPORTANTE",
        ))

    return not found_bad, divergences


def _check_commodity_specs(text: str, commodity_key: str) -> list[SpecDivergence]:
    """Verifica specs tecnicas da commodity vs. ground truth (commodities_specs.json)."""
    specs_db = _load_specs()
    commodity_data = specs_db.get("commodities", {}).get(commodity_key, {})
    if not commodity_data:
        return []

    divergences = []
    text_lower = text.lower()
    specs = commodity_data.get("specs", {})

    checks: list[tuple[str, str, Optional[float]]] = []

    if commodity_key in ("soja_gmo", "soja_non_gmo"):
        checks = [
            ("umidade_max_pct", r"moisture[^\d]*(\d+(?:\.\d+)?)\s*%?\s*max", specs.get("umidade_max_pct")),
            ("oleo_min_pct", r"oil[^\d]*(\d+(?:\.\d+)?)\s*%?\s*min", specs.get("oleo_min_pct")),
            ("proteina_min_pct", r"protein[^\d]*(\d+(?:\.\d+)?)\s*%?\s*min", specs.get("proteina_min_pct")),
            ("materia_estranha_max_pct", r"foreign matter[^\d]*(\d+(?:\.\d+)?)\s*%?\s*max", specs.get("materia_estranha_max_pct")),
        ]
    elif commodity_key == "acucar_icumsa45":
        checks = [
            ("icumsa_max", r"icumsa[^\d]*(\d+)\s*max", specs.get("icumsa_max")),
            ("pol_sacarose_min_pct", r"pol[^\d]*(\d+(?:\.\d+)?)\s*%?\s*min", specs.get("pol_sacarose_min_pct")),
            ("umidade_max_pct", r"moisture[^\d]*(\d+(?:\.\d+)?)\s*%?\s*max", specs.get("umidade_max_pct")),
        ]
    elif commodity_key == "milho_amarelo":
        checks = [
            ("umidade_max_pct", r"moisture[^\d]*(\d+(?:\.\d+)?)\s*%?\s*max", specs.get("umidade_max_pct")),
            ("impurezas_max_pct", r"impurit[^\d]*(\d+(?:\.\d+)?)\s*%?\s*max", specs.get("impurezas_max_pct")),
        ]

    for field_key, pattern, expected_val in checks:
        if expected_val is None:
            continue
        m = re.search(pattern, text_lower)
        if m:
            found_val = float(m.group(1))
            if abs(found_val - float(expected_val)) > 0.5:
                divergences.append(SpecDivergence(
                    field=f"Spec: {field_key}",
                    found=f"{found_val}%",
                    expected=f"{expected_val}%",
                    rule_citation=f"GAFTA 100 / commodities_specs.json — {commodity_key}",
                    severity="IMPORTANTE",
                ))

    return divergences


def _check_dlc_terms(text: str) -> list[SpecDivergence]:
    """Verifica termos criticos DLC no documento (UCP 600)."""
    text_lower = text.lower()
    divergences = []

    # Banco Top 50/100
    has_bank_rank = any(kw in text_lower for kw in [
        "top 50", "top 100", "first class bank", "prime bank",
        "internationally recognized bank", "rated bank"
    ])
    if not has_bank_rank:
        divergences.append(SpecDivergence(
            field="DLC — Classificacao do Banco Emitente",
            found="Nenhuma restricao de ranking bancario detectada",
            expected="Top 50/100 World Bank por ativos (Bloomberg/FT)",
            rule_citation="UCP 600 ICC Pub. 600 — banco emitente qualificado",
            severity="CRITICA",
        ))

    # Prazo emissao DLC
    prazo_match = re.search(
        r"(\d+)\s*(?:banking\s*)?days?\s*(?:after|from|of)\s*(?:spa|agreement|execution)",
        text_lower
    )
    if prazo_match:
        prazo = int(prazo_match.group(1))
        if prazo > 10:
            divergences.append(SpecDivergence(
                field="DLC — Prazo de Emissao",
                found=f"{prazo} dias apos SPA",
                expected="Maximo 7-10 dias uteis apos execucao do SPA",
                rule_citation="UCP 600 — prazo padrao de emissao de DLC",
                severity="IMPORTANTE",
            ))

    # Seguro 110%
    insurance_match = re.search(
        r"(\d+(?:\.\d+)?)\s*%\s*(?:of\s*(?:the\s*)?invoice|cif\s*value|invoice\s*value)",
        text_lower
    )
    if insurance_match:
        pct = float(insurance_match.group(1))
        if pct < 110.0:
            divergences.append(SpecDivergence(
                field="Seguro — Percentual Minimo",
                found=f"{pct}% do valor da fatura",
                expected="Minimo 110% do valor CIF",
                rule_citation="UCP 600 Art. 28 — documento de seguro e cobertura",
                severity="CRITICA",
            ))

    return divergences


def _compute_score(missing: list[MissingClause], divergences: list[SpecDivergence]) -> int:
    """Score de conformidade 0-100."""
    severity_weights = {"CRITICA": 15, "IMPORTANTE": 8, "RECOMENDADA": 3}
    deductions = sum(severity_weights.get(c.severity, 5) for c in missing)
    deductions += sum(severity_weights.get(d.severity, 5) // 2 for d in divergences)
    return max(0, 100 - deductions)


def _score_to_status(score: int, missing: list[MissingClause]) -> ComplianceStatus:
    """Converte score em VERDE/AMARELO/VERMELHO."""
    criticas = sum(1 for c in missing if c.severity == "CRITICA")
    if criticas >= 3 or score < 50:
        return ComplianceStatus.VERMELHO
    elif criticas >= 1 or score < 75:
        return ComplianceStatus.AMARELO
    return ComplianceStatus.VERDE


# ─────────────────────────────────────────────────────────────────────────────
# DocumentalAgent
# ─────────────────────────────────────────────────────────────────────────────

class DocumentalAgent:
    """
    Motor de conformidade documental da Samba Export.

    Auditoria em 5 camadas:
      1. Extracao de texto (PDF / DOCX / TXT)
      2. Clausulas obrigatorias por tipo de documento
      3. Pronomes corporativos (regra ICC: 'its' nunca 'his'/'her')
      4. Especificacoes tecnicas vs. commodities_specs.json
      5. Termos criticos DLC (UCP 600 Arts. 2, 14, 18, 20, 28)
    """

    def auditar_documento(
        self,
        file_path: str,
        expected_type: str,
        commodity: Optional[str] = None,
        save_to_db: bool = True,
        deal_id: Optional[int] = None,
    ) -> ComplianceReport:
        """
        Audita um documento e retorna ComplianceReport completo.

        Args:
            file_path     : Caminho do arquivo (PDF, DOCX, TXT)
            expected_type : Tipo esperado (LOI, ICPO, FCO, SPA, NCNDA, IMFPA)
            commodity     : Chave da commodity (soja_gmo, acucar_icumsa45, etc.)
            save_to_db    : Se True, persiste na tabela document_compliance
            deal_id       : ID do deal associado (opcional)

        Returns:
            ComplianceReport com status, missing_clauses, spec_divergences
        """
        file_path = str(file_path)
        file_name = Path(file_path).name
        doc_type_str = expected_type.upper()

        # 1. Extracao de texto
        try:
            raw_text = _extract_text(file_path)
        except RuntimeError as e:
            return self._error_report(doc_type_str, file_name, commodity, str(e))

        if not raw_text.strip():
            return self._error_report(
                doc_type_str, file_name, commodity,
                "Documento vazio ou texto nao extraivel (PDF escaneado sem OCR?)"
            )

        text_lower = raw_text.lower()
        all_missing: list[MissingClause] = []
        all_divergences: list[SpecDivergence] = []

        # 2. Clausulas obrigatorias
        all_missing.extend(_check_clauses(raw_text, doc_type_str))

        # 3. Pronome corporativo
        pronoun_ok, pronoun_divs = _check_pronoun(raw_text)
        all_divergences.extend(pronoun_divs)

        # 4. Specs de commodity
        if commodity:
            all_divergences.extend(_check_commodity_specs(raw_text, commodity))

        # 5. Termos DLC (documentos com pagamento)
        if doc_type_str in ("LOI", "ICPO", "FCO", "SPA"):
            all_divergences.extend(_check_dlc_terms(raw_text))

        # Deteccao basica de partes
        seller_id = any(kw in text_lower for kw in ["seller", "vendor", "vendedor"])
        buyer_id = any(kw in text_lower for kw in ["buyer", "purchaser", "comprador"])
        incoterm_ok = any(it in text_lower for it in ["cif", "fob", "cfr", "dap", "ddp"])
        payment_ok = any(kw in text_lower for kw in ["letter of credit", "dlc", "ucp 600"])
        dlc_bank_ok = not any(d.field == "DLC — Classificacao do Banco Emitente" for d in all_divergences)

        # Score e status
        score = _compute_score(all_missing, all_divergences)
        status = _score_to_status(score, all_missing)

        # Sumario executivo
        n_criticas = sum(1 for c in all_missing if c.severity == "CRITICA")
        summary_parts = [
            f"Auditoria {doc_type_str} — '{file_name}'.",
            f"Score: {score}/100. Status: {status.value}.",
        ]
        if n_criticas:
            summary_parts.append(f"{n_criticas} clausula(s) CRITICA(s) ausente(s).")
        if all_divergences:
            summary_parts.append(f"{len(all_divergences)} divergencia(s) detectada(s).")
        if status == ComplianceStatus.VERDE:
            summary_parts.append("Documento aprovado — clausulas essenciais presentes.")
        elif status == ComplianceStatus.AMARELO:
            summary_parts.append("Utilizavel com ressalvas. Revisar antes de assinar.")
        else:
            summary_parts.append("REPROVADO. Clausulas criticas ausentes. NAO assinar.")

        recommendations = []
        for c in all_missing:
            if c.severity == "CRITICA":
                recommendations.append(f"[CRITICO] Incluir: {c.clause_name} ({c.rule_citation})")
        for d in all_divergences:
            if d.severity == "CRITICA":
                recommendations.append(f"[CRITICO] Corrigir: {d.field} — {d.rule_citation}")
        for c in all_missing:
            if c.severity == "IMPORTANTE":
                recommendations.append(f"[IMPORTANTE] Incluir: {c.clause_name}")
        for d in all_divergences:
            if d.severity == "IMPORTANTE":
                recommendations.append(
                    f"[IMPORTANTE] Revisar: {d.field} "
                    f"(encontrado: {d.found} | esperado: {d.expected})"
                )

        report = ComplianceReport(
            document_type=DocumentType(doc_type_str),
            file_name=file_name,
            commodity=commodity,
            audit_date=datetime.utcnow().isoformat(),
            status=status,
            score=score,
            seller_identified=seller_id,
            buyer_identified=buyer_id,
            corporate_pronoun_ok=pronoun_ok,
            incoterm_ok=incoterm_ok,
            payment_instrument_ok=payment_ok,
            dlc_bank_rank_ok=dlc_bank_ok,
            specs_match_ground_truth=not any("Spec:" in d.field for d in all_divergences),
            missing_clauses=all_missing,
            spec_divergences=all_divergences,
            summary=" ".join(summary_parts),
            recommendations=recommendations[:10],
            raw_text_preview=raw_text[:500],
        )

        if save_to_db:
            try:
                self._save_compliance(report, file_path, deal_id)
            except Exception as exc:
                print(f"[DocumentalAgent] Aviso — banco: {exc}")

        return report

    @staticmethod
    def _error_report(
        doc_type_str: str, file_name: str, commodity: Optional[str], msg: str
    ) -> ComplianceReport:
        return ComplianceReport(
            document_type=DocumentType(doc_type_str),
            file_name=file_name,
            commodity=commodity,
            audit_date=datetime.utcnow().isoformat(),
            status=ComplianceStatus.VERMELHO,
            score=0,
            seller_identified=False,
            buyer_identified=False,
            missing_clauses=[MissingClause(
                clause_name="Extracao / Conteudo",
                description=msg,
                rule_citation="Prerequisito tecnico",
                severity="CRITICA",
            )],
            spec_divergences=[],
            summary=f"Falha: {msg}",
        )

    def _save_compliance(
        self,
        report: ComplianceReport,
        file_path: str,
        deal_id: Optional[int],
    ) -> None:
        session = get_session()
        try:
            from models.database import DocumentCompliance
            record = DocumentCompliance(
                deal_id=deal_id,
                file_name=report.file_name,
                file_path=str(file_path),
                document_type=report.document_type.value,
                commodity=report.commodity,
                status=report.status.value,
                score=report.score,
                missing_clauses_count=len(report.missing_clauses),
                spec_divergences_count=len(report.spec_divergences),
                critical_issues=report.critical_issues_count,
                summary=report.summary,
                report_json=json.dumps({
                    "missing_clauses": [c.model_dump() for c in report.missing_clauses],
                    "spec_divergences": [d.model_dump() for d in report.spec_divergences],
                    "recommendations": report.recommendations,
                }, ensure_ascii=False),
                audited_at=datetime.utcnow(),
            )
            session.add(record)
            session.commit()
        finally:
            session.close()

    def listar_auditorias(self, deal_id: Optional[int] = None) -> list[dict]:
        """Lista auditorias salvas, opcionalmente filtradas por deal_id."""
        session = get_session()
        try:
            from models.database import DocumentCompliance
            q = session.query(DocumentCompliance)
            if deal_id is not None:
                q = q.filter(DocumentCompliance.deal_id == deal_id)
            records = q.order_by(DocumentCompliance.audited_at.desc()).all()
            return [
                {
                    "id": r.id,
                    "deal_id": r.deal_id,
                    "file_name": r.file_name,
                    "document_type": r.document_type,
                    "commodity": r.commodity,
                    "status": r.status,
                    "score": r.score,
                    "critical_issues": r.critical_issues,
                    "summary": r.summary,
                    "audited_at": r.audited_at.isoformat() if r.audited_at else None,
                }
                for r in records
            ]
        finally:
            session.close()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _print_report(report: ComplianceReport) -> None:
    colors = {"VERDE": "\033[92m", "AMARELO": "\033[93m", "VERMELHO": "\033[91m"}
    RESET = "\033[0m"
    color = colors.get(report.status.value, "")

    print(f"\n{'='*70}")
    print("  RELATORIO DE CONFORMIDADE — Samba Export DocumentalAgent")
    print(f"{'='*70}")
    print(f"  Arquivo   : {report.file_name}")
    print(f"  Tipo      : {report.document_type.value}")
    print(f"  Commodity : {report.commodity or '—'}")
    print(f"  Data      : {report.audit_date}")
    print(f"  Status    : {color}{report.status.value}{RESET}")
    print(f"  Score     : {report.score}/100")
    print(f"  {report.summary}")

    if report.missing_clauses:
        print(f"\n  CLAUSULAS AUSENTES ({len(report.missing_clauses)}):")
        for c in report.missing_clauses:
            col = "\033[91m" if c.severity == "CRITICA" else "\033[93m"
            print(f"    {col}[{c.severity}]{RESET} {c.clause_name}")
            print(f"            Norma: {c.rule_citation}")

    if report.spec_divergences:
        print(f"\n  DIVERGENCIAS ({len(report.spec_divergences)}):")
        for d in report.spec_divergences:
            col = "\033[91m" if d.severity == "CRITICA" else "\033[93m"
            print(f"    {col}[{d.severity}]{RESET} {d.field}")
            print(f"            Encontrado: {d.found}")
            print(f"            Esperado  : {d.expected}")
            print(f"            Norma     : {d.rule_citation}")

    if report.recommendations:
        print(f"\n  RECOMENDACOES:")
        for i, r in enumerate(report.recommendations, 1):
            print(f"    {i}. {r}")

    print(f"{'='*70}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DocumentalAgent — Auditoria ICC/UCP600 — Samba Export"
    )
    parser.add_argument("--file", "-f", required=True, help="Caminho do documento")
    parser.add_argument(
        "--type", "-t", required=True,
        choices=["LOI", "ICPO", "FCO", "SPA", "NCNDA", "IMFPA"],
        help="Tipo de documento"
    )
    parser.add_argument(
        "--commodity", "-c", default=None,
        choices=["soja_gmo", "soja_non_gmo", "acucar_icumsa45", "milho_amarelo", "oleo_soja_degomado"],
        help="Commodity para verificar specs tecnicas"
    )
    parser.add_argument("--no-db", action="store_true", help="Nao salvar no banco")
    parser.add_argument("--deal-id", type=int, default=None)
    args = parser.parse_args()

    agent = DocumentalAgent()
    report = agent.auditar_documento(
        file_path=args.file,
        expected_type=args.type,
        commodity=args.commodity,
        save_to_db=not args.no_db,
        deal_id=args.deal_id,
    )
    _print_report(report)
    sys.exit(0 if report.is_approved else 1)
