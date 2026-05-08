"""
models/document_schemas.py
===========================
Pydantic v2 schemas para validacao documental.

Cobre os 6 tipos de documentos contratuais da Samba Export:
  - LOI  (Letter of Intent)
  - ICPO (Irrevocable Corporate Purchase Order)
  - FCO  (Full Corporate Offer)
  - SPA  (Sale and Purchase Agreement)
  - NCNDA (Non-Circumvention Non-Disclosure Agreement)
  - IMFPA (Irrevocable Master Fee Protection Agreement)

Regras-base:
  * ICC/UCP 600 (Publication No. 600)
  * Incoterms 2020 (ICC Publication No. 723)
  * GAFTA Contract No. 100 (graos/oleaginosas)
  * Pronome corporativo: SEMPRE "its", NUNCA "his"/"her" — regra ICC
"""
from __future__ import annotations

from enum import Enum
from typing import Annotated, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ─────────────────────────────────────────────────────────────────────────────
# Enums compartilhados
# ─────────────────────────────────────────────────────────────────────────────

class Incoterm(str, Enum):
    CIF = "CIF"
    FOB = "FOB"
    CFR = "CFR"
    DAP = "DAP"
    DDP = "DDP"
    FCA = "FCA"
    CPT = "CPT"
    CIP = "CIP"
    EXW = "EXW"


class PaymentInstrument(str, Enum):
    DLC = "DLC"
    TT = "TT"
    USANCE_DLC = "USANCE_DLC"
    STANDBY_LC = "STANDBY_LC"


class ComplianceStatus(str, Enum):
    VERDE = "VERDE"      # Compliant — todas as clausulas presentes e corretas
    AMARELO = "AMARELO"  # Avisos — clausulas importantes faltando, mas documento utilizavel
    VERMELHO = "VERMELHO"  # Reprovado — clausulas criticas ausentes, nao utilizavel


class DocumentType(str, Enum):
    LOI = "LOI"
    ICPO = "ICPO"
    FCO = "FCO"
    SPA = "SPA"
    NCNDA = "NCNDA"
    IMFPA = "IMFPA"


class InspectionEntity(str, Enum):
    SGS = "SGS"
    CCIC = "CCIC"
    BUREAU_VERITAS = "Bureau Veritas"
    INTERTEK = "Intertek"
    INSPECTORATE = "Inspectorate"


# ─────────────────────────────────────────────────────────────────────────────
# Componentes reutilizaveis
# ─────────────────────────────────────────────────────────────────────────────

class PartySchema(BaseModel):
    """Identificacao completa de uma das partes do contrato."""
    company_name: str = Field(..., description="Razao social completa")
    country: str = Field(..., description="Pais de constituicao")
    registration_number: Optional[str] = Field(None, description="CNPJ, registration no., etc.")
    address: Optional[str] = Field(None, description="Endereco completo")
    representative_name: Optional[str] = Field(None, description="Nome do representante autorizado")
    representative_title: Optional[str] = Field(None, description="Cargo/titulo do representante")
    board_resolution_ref: Optional[str] = Field(None, description="Referencia da board resolution ou POA")

    @field_validator("company_name", mode="before")
    @classmethod
    def strip_name(cls, v: str) -> str:
        return v.strip() if v else v


class DLCTermsSchema(BaseModel):
    """Condicoes da Carta de Credito Documentaria — UCP 600."""
    instrument: PaymentInstrument = Field(PaymentInstrument.DLC, description="Instrumento de pagamento")
    transferable: bool = Field(True, description="DLC transferivel (obrigatorio)")
    irrevocable: bool = Field(True, description="DLC irrevogavel (obrigatorio)")
    divisible: bool = Field(True, description="DLC divisivel (obrigatorio)")
    fully_operative: bool = Field(True, description="DLC totalmente operativa (obrigatorio)")
    issuing_bank_min_rank: str = Field("Top 50/100 World Bank", description="Ranking minimo do banco emitente")
    currency: str = Field("USD", description="Moeda da DLC")
    issuance_days_after_spa: int = Field(7, description="Prazo em dias uteis para emissao apos SPA")
    max_issuance_days_after_spa: int = Field(10, description="Prazo maximo em dias uteis")
    validity_min_days: int = Field(90, description="Validade minima em dias")
    ucp_version: str = Field("UCP 600", description="Versao das regras aplicaveis")

    @field_validator("issuance_days_after_spa")
    @classmethod
    def validate_issuance(cls, v: int) -> int:
        if v > 14:
            raise ValueError("Prazo de emissao da DLC nao pode exceder 14 dias uteis")
        return v


class CommoditySpecRef(BaseModel):
    """Referencia a commodity e suas especificacoes no contrato."""
    name: str = Field(..., description="Nome comercial da commodity")
    grade: Optional[str] = Field(None, description="Grau/qualidade (ex: Grade A)")
    hs_code: Optional[str] = Field(None, description="Codigo HS da mercadoria")
    origin: str = Field(..., description="Pais de origem")
    gmo_status: Optional[str] = Field(None, description="GMO / Non-GMO / N/A")
    quality_standard: Optional[str] = Field(None, description="Ex: GAFTA 100, ANEC, FOSFA 53")
    volume_mt: Optional[float] = Field(None, gt=0, description="Volume total em MT")
    price_usd_mt: Optional[float] = Field(None, gt=0, description="Preco em USD/MT")
    incoterm: Optional[Incoterm] = Field(None, description="Incoterm aplicavel")
    port_of_loading: Optional[str] = Field(None, description="Porto de embarque")


# ─────────────────────────────────────────────────────────────────────────────
# LOI — Letter of Intent
# ─────────────────────────────────────────────────────────────────────────────

class LOISchema(BaseModel):
    """
    Schema de validacao para LOI (Letter of Intent).
    Sequencia 1 no processo: LOI > FCO > POF > ICPO > SPA > DLC > Embarque.
    """
    document_type: DocumentType = Field(DocumentType.LOI)
    ref_number: Optional[str] = Field(None, description="Numero de referencia do documento")
    issue_date: Optional[str] = Field(None, description="Data de emissao (DD/MM/YYYY)")
    validity_days: Optional[int] = Field(None, description="Validade em dias (padrao: 3)")

    buyer: Optional[PartySchema] = Field(None, description="Identificacao do comprador")
    seller: Optional[PartySchema] = Field(None, description="Identificacao do vendedor")

    commodity: Optional[CommoditySpecRef] = Field(None)
    payment_instrument: Optional[PaymentInstrument] = Field(
        PaymentInstrument.DLC, description="Instrumento de pagamento proposto"
    )
    dlc_terms: Optional[DLCTermsSchema] = Field(None)
    soft_probe_authorized: Optional[bool] = Field(
        None, description="Autorizacao para Soft Probe (POF)"
    )
    has_confidentiality_clause: Optional[bool] = Field(
        None, description="Referencia a NCNDA presente"
    )
    has_signature: Optional[bool] = Field(None, description="Assinatura e carimbo presentes")

    # Clausulas obrigatorias detectadas (preenchidas pelo DocumentalAgent)
    missing_clauses: list[str] = Field(default_factory=list)
    # Divergencias nas especificacoes vs. ground truth
    spec_divergences: list[str] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# ICPO — Irrevocable Corporate Purchase Order
# ─────────────────────────────────────────────────────────────────────────────

class ICPOSchema(BaseModel):
    """
    Schema de validacao para ICPO (Irrevocable Corporate Purchase Order).
    Sequencia 2 no processo.
    """
    document_type: DocumentType = Field(DocumentType.ICPO)
    ref_number: Optional[str] = Field(None)
    issue_date: Optional[str] = Field(None)
    validity_days: Optional[int] = Field(None, description="Padrao: 5 dias")
    transaction_code: Optional[str] = Field(None, description="TCN — Transaction Code")

    buyer: Optional[PartySchema] = Field(None)
    seller: Optional[PartySchema] = Field(None)

    commodity: Optional[CommoditySpecRef] = Field(None)
    dlc_terms: Optional[DLCTermsSchema] = Field(None)

    # Coordenadas bancarias do comprador
    buyer_bank_name: Optional[str] = Field(None)
    buyer_bank_swift: Optional[str] = Field(None)
    buyer_bank_account: Optional[str] = Field(None)

    # Compromissos criticos do ICPO
    soft_probe_authorized: Optional[bool] = Field(
        None, description="Autorizacao para Soft Probe/POF incluida"
    )
    performance_bond_pct: Optional[float] = Field(
        None, description="Percentual do performance bond (deve ser 2%)"
    )
    board_resolution_attached: Optional[bool] = Field(
        None, description="Board resolution ou POA em anexo"
    )
    has_confidentiality_clause: Optional[bool] = Field(None)
    has_signature: Optional[bool] = Field(None)

    missing_clauses: list[str] = Field(default_factory=list)
    spec_divergences: list[str] = Field(default_factory=list)

    @field_validator("performance_bond_pct")
    @classmethod
    def validate_bond(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v != 2.0:
            # Nao levanta erro — DocumentalAgent vai registrar como divergencia
            pass
        return v


# ─────────────────────────────────────────────────────────────────────────────
# FCO — Full Corporate Offer
# ─────────────────────────────────────────────────────────────────────────────

class FCOSchema(BaseModel):
    """
    Schema de validacao para FCO (Full Corporate Offer).
    Sequencia 3 no processo.

    Baseado na estrutura real do FCO Rokane Soybean da Samba Export
    (FCO_ROKA_20260417_SOYBEAN_GSE_REV03_GLOBAL.pdf).
    """
    document_type: DocumentType = Field(DocumentType.FCO)
    ref_number: Optional[str] = Field(None)
    issue_date: Optional[str] = Field(None)
    validity_days: Optional[int] = Field(None, description="Padrao: 5 dias")

    seller: Optional[PartySchema] = Field(None)
    buyer: Optional[PartySchema] = Field(None)

    # Secao 1 — Condicoes comerciais
    commodity: Optional[CommoditySpecRef] = Field(None)
    dlc_terms: Optional[DLCTermsSchema] = Field(None)

    # Secao 2 — Especificacoes (checagem vs. ground truth commodities_specs.json)
    specs_provided: Optional[bool] = Field(None, description="Tabela de specs presente")

    # Secao 4 — Procedimento de transacao
    transaction_procedure_present: Optional[bool] = Field(
        None, description="Sequencia CIS>FCO>POF>SPA>DLC>Embarque presente"
    )

    # Secao 5 — Outras consideracoes
    performance_bond_pct: Optional[float] = Field(
        None, description="Performance bond do comprador (deve ser 2%)"
    )
    force_majeure_clause: Optional[bool] = Field(None)

    # Secao 6 — Inspecao
    inspection_entity: Optional[InspectionEntity] = Field(None)
    inspection_cost_split: Optional[str] = Field(None, description="Ex: 50/50")
    weight_tolerance_pct: Optional[float] = Field(None, description="Padrao: ±5%")

    # Secao 7 — Custo compartilhado
    cost_split_table_present: Optional[bool] = Field(None)

    # Secao 8 — Documentos de embarque
    required_docs_present: Optional[bool] = Field(
        None, description="Lista de documentos exigidos presente"
    )
    insurance_pct: Optional[float] = Field(
        None, description="Percentual de seguro (deve ser >= 110%)"
    )
    insurance_in_buyer_name: Optional[bool] = Field(
        None, description="Seguro em nome do comprador"
    )
    insurance_clause: Optional[str] = Field(
        None, description="Clausula de seguro (deve ser ICC A ou All Risks)"
    )

    # Secao 9 — Coordenadas bancarias
    seller_bank_coordinates_present: Optional[bool] = Field(None)

    # Secao 10 — Confidencialidade
    confidentiality_clause: Optional[bool] = Field(None)
    ncnda_reference: Optional[bool] = Field(None)
    imfpa_reference: Optional[bool] = Field(None)

    # Governanca
    governing_standard: Optional[str] = Field(
        None, description="Ex: GAFTA 100, UCP 600, Incoterms 2020"
    )
    arbitration_clause: Optional[bool] = Field(None)
    has_signature: Optional[bool] = Field(None)

    missing_clauses: list[str] = Field(default_factory=list)
    spec_divergences: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_insurance(self) -> "FCOSchema":
        if self.insurance_pct is not None and self.insurance_pct < 110.0:
            self.spec_divergences.append(
                f"Seguro {self.insurance_pct}% abaixo do minimo UCP 600 Art.28 (110%)"
            )
        return self

    @model_validator(mode="after")
    def validate_performance_bond(self) -> "FCOSchema":
        if self.performance_bond_pct is not None and self.performance_bond_pct != 2.0:
            self.spec_divergences.append(
                f"Performance bond {self.performance_bond_pct}% diverge do padrao Samba (2%)"
            )
        return self


# ─────────────────────────────────────────────────────────────────────────────
# SPA — Sale and Purchase Agreement
# ─────────────────────────────────────────────────────────────────────────────

class SPASchema(BaseModel):
    """
    Schema de validacao para SPA (Sale and Purchase Agreement).
    Sequencia 4 (documento vinculante definitivo).
    """
    document_type: DocumentType = Field(DocumentType.SPA)
    ref_number: Optional[str] = Field(None)
    issue_date: Optional[str] = Field(None)
    place_of_execution: Optional[str] = Field(None)

    seller: Optional[PartySchema] = Field(None)
    buyer: Optional[PartySchema] = Field(None)

    commodity: Optional[CommoditySpecRef] = Field(None)
    delivery_schedule_present: Optional[bool] = Field(
        None, description="Cronograma de entregas presente"
    )

    dlc_terms: Optional[DLCTermsSchema] = Field(None)

    # Performance bond — critico no SPA
    performance_bond_pct: Optional[float] = Field(None, description="Deve ser 2%")
    performance_bond_days_to_wire: Optional[int] = Field(
        None, description="Dias uteis para envio do PB apos SPA"
    )
    non_delivery_penalty_pct_per_day: Optional[float] = Field(
        None, description="Penalidade por atraso (padrao: 0.5%/dia)"
    )
    max_penalty_pct: Optional[float] = Field(
        None, description="Penalidade maxima total (padrao: 5%)"
    )

    # Inspecao e seguro
    inspection_entity: Optional[InspectionEntity] = Field(None)
    insurance_pct: Optional[float] = Field(None)
    insurance_clause: Optional[str] = Field(None)
    insurance_in_buyer_name: Optional[bool] = Field(None)

    # Clausulas legais
    force_majeure_clause: Optional[bool] = Field(None)
    governing_law: Optional[str] = Field(None)
    arbitration_clause: Optional[bool] = Field(None)
    ncnda_incorporated: Optional[bool] = Field(None, description="NCNDA incorporada por referencia")
    imfpa_incorporated: Optional[bool] = Field(None, description="IMFPA incorporada por referencia")
    entire_agreement_clause: Optional[bool] = Field(None)

    # Execucao
    signed_by_seller: Optional[bool] = Field(None)
    signed_by_buyer: Optional[bool] = Field(None)
    witnesses_present: Optional[bool] = Field(None)
    annexes_listed: Optional[bool] = Field(None)

    missing_clauses: list[str] = Field(default_factory=list)
    spec_divergences: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_critical_fields(self) -> "SPASchema":
        if self.performance_bond_pct is not None and self.performance_bond_pct != 2.0:
            self.spec_divergences.append(
                f"[SPA] Performance bond {self.performance_bond_pct}% diverge do padrao (2%)"
            )
        if self.insurance_pct is not None and self.insurance_pct < 110.0:
            self.spec_divergences.append(
                f"[SPA/UCP600 Art.28] Seguro {self.insurance_pct}% < 110% obrigatorio"
            )
        if self.non_delivery_penalty_pct_per_day is not None:
            if self.non_delivery_penalty_pct_per_day != 0.5:
                self.spec_divergences.append(
                    f"[SPA] Penalidade diaria {self.non_delivery_penalty_pct_per_day}%/dia "
                    f"difere do padrao Samba (0.5%/dia)"
                )
        return self


# ─────────────────────────────────────────────────────────────────────────────
# NCNDA — Non-Circumvention Non-Disclosure Agreement
# ─────────────────────────────────────────────────────────────────────────────

class NCNDASchema(BaseModel):
    """Schema de validacao para NCNDA."""
    document_type: DocumentType = Field(DocumentType.NCNDA)
    ref_number: Optional[str] = Field(None)
    issue_date: Optional[str] = Field(None)

    parties: list[PartySchema] = Field(default_factory=list, description="Todas as partes do NCNDA")

    # Clausulas criticas
    has_non_circumvention: Optional[bool] = Field(None, description="Clausula de nao-circunvencao")
    has_non_disclosure: Optional[bool] = Field(None, description="Clausula de confidencialidade")
    has_penalty_clause: Optional[bool] = Field(None, description="Penalidade por violacao definida")
    penalty_amount_usd: Optional[float] = Field(None, description="Valor da penalidade em USD")
    penalty_pct_of_deal: Optional[float] = Field(
        None, description="Percentual do valor do negocio (padrao: 5%)"
    )
    validity_years: Optional[int] = Field(None, description="Vigencia (minimo 2 anos)")
    survival_clause: Optional[bool] = Field(
        None, description="Clausula de sobrevivencia apos termino"
    )
    governing_law: Optional[str] = Field(None)
    signed_by_all_parties: Optional[bool] = Field(None)

    missing_clauses: list[str] = Field(default_factory=list)
    spec_divergences: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_ncnda(self) -> "NCNDASchema":
        if self.validity_years is not None and self.validity_years < 2:
            self.spec_divergences.append(
                f"[NCNDA] Vigencia {self.validity_years} ano(s) abaixo do minimo recomendado (2 anos)"
            )
        if self.penalty_pct_of_deal is not None and self.penalty_pct_of_deal < 5.0:
            self.spec_divergences.append(
                f"[NCNDA] Penalidade {self.penalty_pct_of_deal}% abaixo do padrao Samba (5%)"
            )
        return self


# ─────────────────────────────────────────────────────────────────────────────
# IMFPA — Irrevocable Master Fee Protection Agreement
# ─────────────────────────────────────────────────────────────────────────────

class IMFPASchema(BaseModel):
    """Schema de validacao para IMFPA."""
    document_type: DocumentType = Field(DocumentType.IMFPA)
    ref_number: Optional[str] = Field(None)
    issue_date: Optional[str] = Field(None)
    transaction_ref: Optional[str] = Field(None, description="Referencia ao SPA ou TCN")

    seller: Optional[PartySchema] = Field(None)
    buyer: Optional[PartySchema] = Field(None)
    intermediaries: list[PartySchema] = Field(default_factory=list)

    # Estrutura de taxas
    total_fee_pct: Optional[float] = Field(None, description="Percentual total de comissoes")
    fee_breakdown_present: Optional[bool] = Field(
        None, description="Tabela de breakdown de comissoes presente"
    )
    bank_coordinates_present: Optional[bool] = Field(
        None, description="Coordenadas bancarias de todos os beneficiarios presentes"
    )

    # Clausulas de irrevogabilidade
    irrevocability_clause: Optional[bool] = Field(None, description="Clausula de irrevogabilidade")
    simultaneous_payment_clause: Optional[bool] = Field(
        None, description="Pagamento simultaneo ao principal"
    )
    anti_circumvention_clause: Optional[bool] = Field(
        None, description="Protecao contra circunvencao de intermediarios"
    )
    survival_on_renewal: Optional[bool] = Field(
        None, description="IMFPA sobrevive em renovacoes do SPA"
    )

    signed_by_all_parties: Optional[bool] = Field(None)
    governing_law: Optional[str] = Field(None)

    missing_clauses: list[str] = Field(default_factory=list)
    spec_divergences: list[str] = Field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# ComplianceReport — resultado final da auditoria
# ─────────────────────────────────────────────────────────────────────────────

class SpecDivergence(BaseModel):
    """Uma divergencia especifica encontrada no documento."""
    field: str = Field(..., description="Campo ou secao afetada")
    found: str = Field(..., description="Valor encontrado no documento")
    expected: str = Field(..., description="Valor esperado pelo padrao")
    rule_citation: str = Field(..., description="Norma ou artigo violado (ex: UCP 600 Art. 28)")
    severity: str = Field(..., description="CRITICA | IMPORTANTE | INFORMATIVA")


class MissingClause(BaseModel):
    """Uma clausula obrigatoria ausente no documento."""
    clause_name: str = Field(..., description="Nome da clausula ausente")
    description: str = Field(..., description="O que esta clausula deveria conter")
    rule_citation: str = Field(..., description="Norma que exige esta clausula")
    severity: str = Field(..., description="CRITICA | IMPORTANTE | RECOMENDADA")


class ComplianceReport(BaseModel):
    """
    Relatorio completo de conformidade documental.
    Retornado pelo DocumentalAgent.auditar_documento().
    """
    document_type: DocumentType
    file_name: Optional[str] = Field(None)
    commodity: Optional[str] = Field(None)
    audit_date: str = Field(..., description="Data da auditoria (ISO format)")

    # Veredicto geral
    status: ComplianceStatus = Field(..., description="VERDE | AMARELO | VERMELHO")
    score: int = Field(..., ge=0, le=100, description="Score de conformidade (0-100)")

    # Partes identificadas
    seller_identified: bool = Field(False)
    buyer_identified: bool = Field(False)
    corporate_pronoun_ok: Optional[bool] = Field(
        None, description="Nao usa 'his'/'her' para corporacoes (regra ICC)"
    )

    # Campos criticos
    incoterm_ok: Optional[bool] = Field(None)
    payment_instrument_ok: Optional[bool] = Field(None)
    dlc_bank_rank_ok: Optional[bool] = Field(None)
    specs_match_ground_truth: Optional[bool] = Field(None)

    # Problemas encontrados
    missing_clauses: list[MissingClause] = Field(default_factory=list)
    spec_divergences: list[SpecDivergence] = Field(default_factory=list)

    # Resumo textual gerado por IA
    summary: str = Field("", description="Resumo executivo da auditoria")
    recommendations: list[str] = Field(default_factory=list)

    # Texto extraido do documento (para debugging)
    raw_text_preview: Optional[str] = Field(
        None, description="Primeiros 500 chars do texto extraido"
    )

    @property
    def is_approved(self) -> bool:
        return self.status in (ComplianceStatus.VERDE, ComplianceStatus.AMARELO)

    @property
    def critical_issues_count(self) -> int:
        return (
            sum(1 for c in self.missing_clauses if c.severity == "CRITICA")
            + sum(1 for d in self.spec_divergences if d.severity == "CRITICA")
        )
