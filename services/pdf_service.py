"""
services/pdf_service.py
=======================
Gerador de Ficha de Cadastro em PDF da Samba Export.
Atualizado com as Diretrizes de Marca (Manual 2026) e Logos Oficiais.
"""
from fpdf import FPDF
from datetime import datetime
import os
import logging
from pathlib import Path

logger = logging.getLogger("PDFService")

# Define dinamicamente a pasta raiz (SAMBA_AGENTS)
ROOT = Path(__file__).resolve().parent.parent

# --- CORES DA MARCA SAMBA EXPORT (Extraídas do Manual 2026) ---
COR_PRIMARIA = (250, 130, 0)       # Laranja/Amarelo (#FA8200)
COR_TEXTO = (0, 0, 0)              # Preto 100% (#000000)
COR_TEXTO_LEVE = (64, 64, 64)      # Preto 75%
COR_FUNDO_CELULA = (216, 216, 216) # Preto 15% (Para fundo de tabelas)
COR_DESTAQUE_AZUL = (50, 100, 150) # Azul Escuro de Apoio (#326496)

class PDFService:
    @staticmethod
    def gerar_ficha_pedido(deal_data, output_path):
        try:
            pdf = FPDF()
            pdf.add_page()
            
            # Margens
            pdf.set_left_margin(15)
            pdf.set_right_margin(15)
            pdf.set_top_margin(20)
            
            # 1. FAIXA SUPERIOR (Bleed de topo com a Cor Primária)
            pdf.set_fill_color(*COR_PRIMARIA)
            pdf.rect(0, 0, 210, 8, 'F')
            
            # 2. LOGOTIPO OFICIAL DA SAMBA EXPORT (Horizontal - para cabeçalho de documentos)
            logo_path = os.path.join(ROOT, "logo_samba_horiz.png")

            if os.path.exists(logo_path):
                # Imprime a imagem real: x=15 (margem esq), y=12 (topo), w=45 (largura em mm)
                pdf.image(logo_path, x=15, y=12, w=45)
                pdf.ln(18) # Quebra de linha para não encavalar o título
            else:
                # Fallback de segurança (simulação em texto) caso alguém delete a imagem
                pdf.ln(5)
                pdf.set_font("helvetica", '', 26)
                pdf.set_text_color(*COR_PRIMARIA)
                pdf.cell(38, 10, "samba ", ln=0, align='L')
                
                pdf.set_font("helvetica", 'B', 26)
                pdf.set_text_color(*COR_TEXTO)
                pdf.cell(50, 10, "EXPORT", ln=1, align='L')
            
            # Linha divisória elegante
            pdf.set_draw_color(*COR_PRIMARIA)
            pdf.set_line_width(0.5)
            pdf.line(15, 38, 195, 38)
            pdf.set_line_width(0.2)
            
            # 3. TÍTULO DO DOCUMENTO
            pdf.ln(12)
            pdf.set_font("helvetica", 'B', 14)
            pdf.set_text_color(*COR_DESTAQUE_AZUL)
            pdf.cell(180, 10, "FICHA DE CADASTRO DE PEDIDO", ln=True, align='C')
            pdf.ln(5)
            
            # Tratamento blindado de Data
            data_raw = deal_data.get("created_at")
            if isinstance(data_raw, datetime):
                data_str = data_raw.strftime("%d/%m/%Y")
            else:
                data_str = str(data_raw).split(" ")[0] if data_raw else datetime.now().strftime("%d/%m/%Y")
            
            # 4. TABELA DE DADOS (Padrão Corporativo)
            campos = [
                ("DATA", data_str),
                ("ID DO NEGÓCIO", str(deal_data.get("name", "N/A"))),
                ("PRODUTO", str(deal_data.get("commodity", "AÇÚCAR")).upper()),
                ("SPEC", str(deal_data.get("spec", "TBI"))),
                ("ORIGEM", "Brasil"),
                ("PREÇO", f"{deal_data.get('currency', 'USD')} {deal_data.get('price', 'TBI')} per Mt"),
                ("INCOTERM", str(deal_data.get("incoterm", "TBI"))),
                ("DESTINO", str(deal_data.get("destination", "TBI"))),
                ("CONTRATO", str(deal_data.get("contract_type", "SPOT / EXTENSIVO"))),
                ("COMISSÃO", str(deal_data.get("commission", "TBI"))),
                ("COMPRADOR", str(deal_data.get("buyer", "TBI"))),
                ("GRUPO WAPP", str(deal_data.get("source_group", "TBI"))),
            ]
            
            pdf.set_draw_color(180, 180, 180) # Borda cinza claro
            for label, value in campos:
                pdf.set_font("helvetica", 'B', 10)
                pdf.set_text_color(*COR_TEXTO)
                pdf.set_fill_color(*COR_FUNDO_CELULA)
                pdf.cell(50, 10, f" {label}:", border=1, fill=True)
                
                pdf.set_font("helvetica", '', 10)
                pdf.set_text_color(*COR_TEXTO_LEVE)
                pdf.cell(130, 10, f" {value}", border=1, ln=True)
                
            # 5. RODAPÉ INSTITUCIONAL
            pdf.ln(15)
            pdf.set_font("helvetica", 'I', 8)
            pdf.set_text_color(128, 128, 128)
            pdf.cell(180, 10, "Documento gerado automaticamente pelo Agente de Inteligência Samba Export.", align='C')
            
            pdf.output(output_path)
            logger.info(f"✅ Ficha PDF gerada fisicamente em: {output_path} (Padrão de Marca e Logo Aplicados)")
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Erro fatal ao desenhar a Ficha PDF: {e}")
            raise e