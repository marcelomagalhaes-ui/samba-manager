"""
agents/legacy_sync_agent.py
===========================
Auditor Institucional de Compliance (Retroactive Sync).
Varre a árvore do Drive Compartilhado, identifica pastas de commodities,
ignora pastas de sistema (iniciadas com "_") e formaliza Fichas de Cadastro
onde estiverem faltando.
"""
import sys
import os
import logging
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.drive_service import DriveService
from services.pdf_service import PDFService
from services.gemini_api import extract_quote_data

logging.basicConfig(level=logging.INFO, format='%(levelname)s: [%(name)s] %(message)s')
logger = logging.getLogger("LegacySync")

# A PASTA RAIZ QUE VOCÊ FORNECEU
SAMBA_ROOT_FOLDER_ID = "1k0uKPg7Xyq8MyI8KI1bRKzR-Bow_41B5"

class LegacySyncAgent:
    def __init__(self):
        self.drive = DriveService()
        self.pdf = PDFService()

    def sync_toda_arvore(self):
        logger.info("⚡ Iniciando Auditoria Geral na Raiz da Samba Export...")
        
        # 1. LISTA AS PASTAS DE COMMODITIES (Nível 1)
        commodities_folders = self.drive.listar_subpastas(SAMBA_ROOT_FOLDER_ID)
        logger.info(f"🔎 Encontradas {len(commodities_folders)} pastas no diretório raiz.")

        for comm_folder in commodities_folders:
            comm_name = comm_folder['name']
            comm_id = comm_folder['id']
            
            # REGRA DE GOVERNANÇA: Ignorar pastas que começam com '_'
            if comm_name.startswith('_'):
                logger.info(f"⏭️ Ignorando pasta de sistema/template: {comm_name}")
                continue

            logger.info(f"==================================================")
            logger.info(f"📦 INICIANDO AUDITORIA NA COMMODITY: {comm_name.upper()}")
            logger.info(f"==================================================")
            
            # 2. LISTA AS PASTAS DE NEGÓCIOS DENTRO DA COMMODITY (Nível 2)
            deals_folders = self.drive.listar_subpastas(comm_id)
            logger.info(f"   📂 Encontrados {len(deals_folders)} negócios em {comm_name}")
            
            for deal_folder in deals_folders:
                deal_name = deal_folder['name']
                deal_id = deal_folder['id']
                
                # 3. VERIFICA COMPLIANCE (Tem Ficha?)
                if self.drive.verificar_ficha_existente(deal_id):
                    logger.info(f"   ✅ OK: {deal_name} já possui Ficha.")
                    continue

                logger.info(f"   🚨 PENDÊNCIA: {deal_name} sem ficha. Iniciando reconstrução...")

                # 4. LEITURA DE CONTEXTO E RECONSTRUÇÃO (RAG + IA)
                arquivos = self.drive.ler_contexto_da_pasta(deal_id)
                nomes_arquivos = ", ".join(arquivos) if arquivos else "Nenhum arquivo"
                
                # Prepara um texto descritivo para a IA interpretar
                contexto_texto = (
                    f"Reconstrua um negócio comercial a partir destes metadados.\n"
                    f"Commodity (Produto): {comm_name}\n"
                    f"Nome da Pasta do Negócio (Geralmente contém Remetente e Volume): {deal_name}\n"
                    f"Arquivos presentes na pasta: {nomes_arquivos}"
                )
                
                # Extrai dados via LLM usando o contexto
                try:
                    dados_ia = extract_quote_data(message_text=contexto_texto, sender="Auditoria Drive", group=comm_name)
                    
                    # Se o LLM retornar uma lista (multi-deal), pegamos o primeiro
                    if isinstance(dados_ia, list) and len(dados_ia) > 0:
                        dados_ia = dados_ia[0]
                    elif not isinstance(dados_ia, dict):
                        dados_ia = {}

                except Exception as e:
                    logger.error(f"   ❌ Falha na interpretação da IA para {deal_name}: {e}")
                    dados_ia = {}

                # Garante os dados mínimos vitais
                dados_ia['name'] = deal_name
                dados_ia['commodity'] = comm_name # Força a commodity correta da pasta!
                dados_ia['created_at'] = datetime.now()
                
                # 5. GERA E UPLOAD DA FICHA (Nível 3)
                temp_path = os.path.join(ROOT, "temp", f"FICHA_CADASTRO_{deal_id}.pdf")
                os.makedirs(os.path.dirname(temp_path), exist_ok=True)
                
                try:
                    self.pdf.gerar_ficha_pedido(dados_ia, temp_path)
                    self.drive.upload_arquivo(temp_path, deal_id)
                    logger.info(f"   🚀 SUCESSO: Ficha gerada e enviada para {deal_name}")
                except Exception as e:
                    logger.error(f"   ❌ Falha na geração/upload do PDF para {deal_name}: {e}")

        logger.info("🏁 Auditoria de Compliance (Legacy Sync) Concluída.")

if __name__ == "__main__":
    agente_auditor = LegacySyncAgent()
    agente_auditor.sync_toda_arvore()