# -*- coding: utf-8 -*-
"""
agents/loi_generator_agent.py
==============================
LOIGeneratorAgent — orquestra a geração de LOIs.

Pipeline:
  1. Valida payload (commodity, product, DESTINATARY)
  2. Localiza template no Drive (pasta TEMPLATES_FOLDER_ID)
  3. Baixa bytes (.docx ou Google Doc → exportado como docx)
  4. Constrói RenderContext via build_context
  5. Renderiza com render_loi
  6. Upload na pasta OUTPUT_FOLDER_ID (convertido em Google Doc)
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
from data.knowledge.loi_dictionary import (
    COMMODITIES, get_commodity, get_product,
)
from services.google_drive import DriveManager
from services.loi_template_engine import (
    build_context, build_output_filename, render_loi,
)


TEMPLATES_FOLDER_ID = "1M9GsOrKTBvQxQRde1D1HraS4kVevlg3v"
OUTPUT_FOLDER_ID    = "1CPlF_9TtEZ32B5eTAb4b4jRC-h7L10Z2"


class LOIGeneratorAgent(BaseAgent):
    name = "LOIGeneratorAgent"
    description = (
        "Gera LOIs Samba Export a partir de templates no Drive (7 commodities), "
        "selecionando o produto escolhido e suprimindo os demais via marcadores "
        "no padrão {NomeDoProduto}. Upload final na pasta de saída."
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

    def process(self, data: Any = None) -> dict:
        if not isinstance(data, dict):
            return {"status": "error", "error": "Payload inválido (dict esperado)."}

        commodity_code  = (data.get("commodity_code") or "").upper()
        product_label   = data.get("product_label")
        user_inputs     = data.get("user_inputs", {}) or {}
        dry_run         = bool(data.get("dry_run", False))
        output_format   = (data.get("output_format") or "pdf").lower()   # "pdf" | "gdoc"

        if commodity_code not in COMMODITIES:
            return {
                "status": "error",
                "error": f"Commodity desconhecida: {commodity_code!r}. "
                         f"Use uma de {list(COMMODITIES)}.",
            }

        com = get_commodity(commodity_code)
        labels = [p["label"] for p in com["products"]]
        if product_label not in labels:
            return {
                "status": "error",
                "error": f"Produto inválido para {commodity_code}: {product_label!r}. "
                         f"Disponíveis: {labels}",
            }

        destinatary = (
            user_inputs.get("DESTINATARY_LOIFULLNAME")
            or user_inputs.get("DESTINATARY")
            or ""
        ).strip()
        if not destinatary:
            return {
                "status": "error",
                "error": "DESTINATARY_LOIFULLNAME (ou DESTINATARY) é obrigatório.",
            }

        alerts: List[str] = []

        # Sugar non-Brazil → alerta MAPA
        if commodity_code == "SUGAR":
            origin = user_inputs.get("ORIGIN_COUNTRY", "Brazil")
            if origin and origin != "Brazil":
                alerts.append(com["extra_rules"]["non_brazil_alert"])

        # Localizar template
        template_filename = com["template_filename"]
        self.log_action("locate_template", {"filename": template_filename})

        meta = self.drive.find_file_by_name(
            template_filename, TEMPLATES_FOLDER_ID,
            ignore_underscore_prefix=True,
        )
        if not meta:
            return {
                "status": "error",
                "error": (
                    f"Template '{template_filename}' não encontrado em "
                    f"https://drive.google.com/drive/folders/{TEMPLATES_FOLDER_ID}"
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
            "file_id": meta["id"], "mime_type": meta.get("mimeType"),
            "size_bytes": len(template_bytes),
        })

        # Validação: PACKAGING dentro das opções permitidas
        prod = get_product(commodity_code, product_label)
        chosen_pack = user_inputs.get("PACKAGING")
        if chosen_pack and chosen_pack not in prod.get("packaging_options", []):
            alerts.append(
                f"PACKAGING '{chosen_pack}' fora das opções padrão "
                f"para {product_label}: {prod.get('packaging_options', [])}"
            )

        # Renderizar
        try:
            ctx = build_context(commodity_code, product_label, user_inputs)
            output_bytes = render_loi(template_bytes, ctx)
        except Exception as exc:
            return {"status": "error", "error": f"Renderização falhou: {exc}"}

        # Filename: usa DESTINATARY_LOIFIRSTNAME se fornecido, senão split do full name
        first_name = (
            user_inputs.get("DESTINATARY_LOIFIRSTNAME")
            or (destinatary.split()[0] if destinatary else "DESTINATARY")
        )
        filename = build_output_filename(first_name, commodity_code)

        if dry_run:
            return {
                "status": "success",
                "dry_run": True,
                "filename": filename,
                "size_bytes": len(output_bytes),
                "selected_keywords": ctx.selected_strict_keywords + ctx.selected_family_keywords,
                "drop_keywords": ctx.other_strict_keywords,
                "simple_keys": sorted(ctx.simple_keys.keys()),
                "alerts": alerts,
            }

        # ── Upload ───────────────────────────────────────────────────────
        # Passo 1: sobe como Google Doc (Drive converte o .docx)
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

        # Passo 2: se output_format == "pdf", exporta o Google Doc como PDF
        #          e faz upload do PDF; depois apaga o Google Doc intermediário.
        if output_format == "pdf":
            pdf_bytes = self.drive.export_gdoc_as_pdf_bytes(gdoc["id"])
            if not pdf_bytes:
                # fallback: mantém o Google Doc se a conversão falhar
                self.log_action("loi_pdf_fallback", {"gdoc_id": gdoc["id"]})
            else:
                pdf_filename = filename.replace(".docx", ".pdf")
                uploaded = self.drive.upload_file_bytes(
                    filename=pdf_filename,
                    content=pdf_bytes,
                    folder_id=OUTPUT_FOLDER_ID,
                    mime_type="application/pdf",
                    save_as_google_doc=False,
                )
                # Remove o Google Doc intermediário
                self.drive.delete_file(gdoc["id"])

                if not uploaded:
                    return {
                        "status": "error",
                        "error": "Falha no upload do PDF.",
                        "filename": pdf_filename,
                    }

                self.log_action("loi_uploaded", {
                    "file_id": uploaded.get("id"),
                    "filename": uploaded.get("name"),
                    "format": "pdf",
                })
                return {
                    "status": "success",
                    "file_id":  uploaded.get("id"),
                    "filename": uploaded.get("name"),
                    "web_link": uploaded.get("webViewLink"),
                    "size_bytes": len(pdf_bytes),
                    "format": "pdf",
                    "file_bytes": pdf_bytes,
                    "alerts": alerts,
                }

        # Formato Google Doc (padrão anterior)
        self.log_action("loi_uploaded", {
            "file_id": gdoc.get("id"),
            "filename": gdoc.get("name"),
            "format": "gdoc",
        })
        return {
            "status": "success",
            "file_id":  gdoc.get("id"),
            "filename": gdoc.get("name"),
            "web_link": gdoc.get("webViewLink"),
            "size_bytes": len(output_bytes),
            "format": "gdoc",
            "alerts": alerts,
        }
