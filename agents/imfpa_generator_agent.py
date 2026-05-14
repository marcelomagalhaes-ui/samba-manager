# -*- coding: utf-8 -*-
"""
agents/imfpa_generator_agent.py
================================
IMFPAGeneratorAgent — orquestra a geração de IMFPAs.

Pipeline:
  1. Valida payload (n_parties, dados das partes)
  2. Seleciona template no Drive:
       1IMFPA… → 1 intermediário
       2IMFPA… → 2 intermediários
       3IMFPA… → 3 intermediários
  3. Baixa bytes (.docx / Google Doc exportado)
  4. Constrói IMFPAContext
  5. Renderiza com render_imfpa
  6. Upload na pasta OUTPUT_FOLDER_ID (PDF ou GDoc)
  7. Retorna {status, file_id, web_link, alerts}
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from agents.base_agent import BaseAgent
from services.google_drive import DriveManager
from services.imfpa_template_engine import (
    IMFPAContext,
    build_imfpa_output_filename,
    render_imfpa,
)

# ─── Pastas no Drive ──────────────────────────────────────────────────────────
# Mesmas pastas usadas pelo LOI Generator
TEMPLATES_FOLDER_ID = "1M9GsOrKTBvQxQRde1D1HraS4kVevlg3v"
OUTPUT_FOLDER_ID    = "1CPlF_9TtEZ32B5eTAb4b4jRC-h7L10Z2"

# Nomes completos dos templates IMFPA no Drive
# (podem estar salvos como Google Docs — find_file_by_name tenta com/sem .docx)
_TEMPLATE_NAMES = {
    1: "1IMFPA - SAMBA INTERM DE NEGOCIOS (EN).docx",
    2: "2IMFPA - SAMBA INTERM DE NEGOCIOS (EN).docx",
    3: "3IMFPA - SAMBA INTERM DE NEGOCIOS (EN).docx",
}
# Prefixos para busca tolerante (find_file_by_prefix)
_TEMPLATE_PREFIX = {1: "1IMFPA", 2: "2IMFPA", 3: "3IMFPA"}


class IMFPAGeneratorAgent(BaseAgent):
    name = "IMFPAGeneratorAgent"
    description = (
        "Gera IMFPAs (Irrevocable Master Fee Protection Agreement) "
        "a partir de templates no Drive, selecionando o template de 1, 2 ou 3 partes "
        "conforme o número de intermediários informado."
    )
    visible_in_groups = False
    generates_spreadsheets = False

    def __init__(self, drive: Optional[DriveManager] = None):
        super().__init__()
        self._drive = drive

    @property
    def drive(self) -> DriveManager:
        if self._drive is None:
            self._drive = DriveManager()
        return self._drive

    # ─── process ─────────────────────────────────────────────────────────────
    def process(self, data: Any = None) -> dict:
        if not isinstance(data, dict):
            return {"status": "error", "error": "Payload inválido (dict esperado)."}

        n_parties     = int(data.get("n_parties", 0))
        user_inputs   = data.get("user_inputs", {}) or {}
        dry_run       = bool(data.get("dry_run", False))
        output_format = (data.get("output_format") or "pdf").lower()  # "pdf" | "gdoc"

        # ── Validação básica ──────────────────────────────────────────────────
        if n_parties not in (1, 2, 3):
            return {
                "status": "error",
                "error": "n_parties deve ser 1, 2 ou 3.",
            }

        alerts: List[str] = []

        # ── Construir contexto ────────────────────────────────────────────────
        ctx = _build_context(n_parties, user_inputs)

        if not ctx.doc_code:
            alerts.append("doc_code não informado — marcadores de código não serão substituídos.")

        if not ctx.commodity:
            alerts.append("commodity não informada — literal SOYBEAN não será substituído.")

        # ── Localizar template no Drive ───────────────────────────────────────
        template_filename = _TEMPLATE_NAMES[n_parties]
        self.log_action("locate_template", {"filename": template_filename, "n_parties": n_parties})

        # Tenta match exato primeiro; fallback: busca pelo prefixo "1IMFPA" / "2IMFPA" etc.
        meta = self.drive.find_file_by_name(
            template_filename,
            TEMPLATES_FOLDER_ID,
            ignore_underscore_prefix=True,
        )
        if not meta:
            prefix = _TEMPLATE_PREFIX[n_parties]   # ex: "1IMFPA"
            self.log_action("locate_template_fallback", {"prefix": prefix})
            meta = self.drive.find_file_by_prefix(
                prefix,
                TEMPLATES_FOLDER_ID,
                ignore_underscore_prefix=True,
            )
        if not meta:
            return {
                "status": "error",
                "error": (
                    f"Template '{template_filename}' não encontrado em "
                    f"https://drive.google.com/drive/folders/{TEMPLATES_FOLDER_ID} "
                    f"(busca exata e por prefixo '{_TEMPLATE_PREFIX[n_parties]}' falharam)"
                ),
            }

        template_bytes = self.drive.fetch_as_docx_bytes(meta)
        if not template_bytes:
            return {
                "status": "error",
                "error": (
                    f"Falha ao obter bytes de {meta['name']} "
                    f"({meta.get('id')}, mime={meta.get('mimeType')})"
                ),
            }
        self.log_action("template_downloaded", {
            "file_id": meta["id"],
            "mime_type": meta.get("mimeType"),
            "size_bytes": len(template_bytes),
        })

        # ── Renderizar ────────────────────────────────────────────────────────
        try:
            output_bytes = render_imfpa(template_bytes, ctx)
        except Exception as exc:
            return {"status": "error", "error": f"Renderização falhou: {exc}"}

        filename = build_imfpa_output_filename(ctx.doc_code, n_parties)

        if dry_run:
            return {
                "status": "success",
                "dry_run": True,
                "filename": filename,
                "size_bytes": len(output_bytes),
                "n_parties": n_parties,
                "template_used": meta["name"],
                "replacements": ctx.flat_replacements(),
                "alerts": alerts,
            }

        # ── Upload ────────────────────────────────────────────────────────────
        # Passo 1: sobe como Google Doc
        gdoc = self.drive.upload_file_bytes(
            filename=filename,
            content=output_bytes,
            folder_id=OUTPUT_FOLDER_ID,
            save_as_google_doc=True,
        )
        if not gdoc:
            return {
                "status": "error",
                "error": "Falha no upload para o Drive.",
                "filename": filename,
            }

        # Passo 2: exporta como PDF se solicitado
        if output_format == "pdf":
            pdf_bytes = self.drive.export_gdoc_as_pdf_bytes(gdoc["id"])
            if not pdf_bytes:
                self.log_action("imfpa_pdf_fallback", {"gdoc_id": gdoc["id"]})
            else:
                pdf_filename = filename.replace(".docx", ".pdf")
                uploaded = self.drive.upload_file_bytes(
                    filename=pdf_filename,
                    content=pdf_bytes,
                    folder_id=OUTPUT_FOLDER_ID,
                    mime_type="application/pdf",
                    save_as_google_doc=False,
                )
                self.drive.delete_file(gdoc["id"])

                if not uploaded:
                    return {
                        "status": "error",
                        "error": "Falha no upload do PDF.",
                        "filename": pdf_filename,
                    }

                self.log_action("imfpa_uploaded", {
                    "file_id": uploaded.get("id"),
                    "filename": uploaded.get("name"),
                    "format": "pdf",
                })
                return {
                    "status": "success",
                    "file_id":   uploaded.get("id"),
                    "filename":  uploaded.get("name"),
                    "web_link":  uploaded.get("webViewLink"),
                    "size_bytes": len(pdf_bytes),
                    "format":    "pdf",
                    "file_bytes": pdf_bytes,
                    "alerts":    alerts,
                }

        # Formato Google Doc
        self.log_action("imfpa_uploaded", {
            "file_id": gdoc.get("id"),
            "filename": gdoc.get("name"),
            "format": "gdoc",
        })
        return {
            "status":    "success",
            "file_id":   gdoc.get("id"),
            "filename":  gdoc.get("name"),
            "web_link":  gdoc.get("webViewLink"),
            "size_bytes": len(output_bytes),
            "format":    "gdoc",
            "alerts":    alerts,
        }


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _build_context(n_parties: int, ui: dict) -> IMFPAContext:
    """Constrói IMFPAContext a partir do dicionário user_inputs."""
    ctx = IMFPAContext()

    ctx.date_str         = ui.get("DATE", "")
    ctx.doc_code         = ui.get("DOC_CODE", "")
    ctx.quantity_mt      = ui.get("QUANTITY_MT", "")
    ctx.spa_code         = ui.get("SPA_CODE", "")
    ctx.commodity        = ui.get("COMMODITY", "")
    ctx.fee_per_shipment = ui.get("FEE_PER_SHIPMENT", "")
    ctx.fee_total        = ui.get("FEE_TOTAL", "")

    for n in range(1, n_parties + 1):
        ctx.company_name[n]     = ui.get(f"COMPANY_NAME_{n}", "")
        ctx.country[n]          = ui.get(f"COUNTRY_{n}", "")
        ctx.tax_id[n]           = ui.get(f"TAX_ID_{n}", "")
        ctx.address[n]          = ui.get(f"ADDRESS_{n}", "")
        ctx.legal_rep_name[n]   = ui.get(f"LEGAL_REP_NAME_{n}", "")
        ctx.passport[n]         = ui.get(f"PASSPORT_{n}", "")
        ctx.beneficiary_name[n] = ui.get(f"BENEFICIARY_NAME_{n}", "")
        ctx.doc_number[n]       = ui.get(f"DOC_NUMBER_{n}", "")
        ctx.bank_name[n]        = ui.get(f"BANK_NAME_{n}", "")
        ctx.swift[n]            = ui.get(f"SWIFT_{n}", "")
        ctx.iban[n]             = ui.get(f"IBAN_{n}", "")

    return ctx
