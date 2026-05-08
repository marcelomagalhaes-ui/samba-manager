"""
services/drive_service.py
=========================
Gerenciador de Pastas e Uploads no Google Drive (Shared Drive Support).
Implementação de nível Enterprise com suporte a volumes corporativos e auditoria.
"""
import logging
import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from services.google_drive import drive_manager

logger = logging.getLogger("DriveService")

# ID padrão da pasta AÇÚCAR (Mantido para compatibilidade com o Extractor)
DEFAULT_SUGAR_FOLDER_ID = "1razr9nnam33UuMiLQvsZIwaurUe6An6h"

class DriveService:
    def __init__(self):
        # A autenticação já utiliza o escopo total 'auth/drive'
        self.service = build('drive', 'v3', credentials=drive_manager.creds)

    def criar_pasta_negocio(self, folder_name, parent_id=DEFAULT_SUGAR_FOLDER_ID):
        """
        Cria uma pasta para o Deal em um Drive Compartilhado.
        Permite definir o parent_id dinamicamente (útil para múltiplas commodities).
        """
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id]
        }
        try:
            # CRITICAL: supportsAllDrives=True é obrigatório para Shared Drives
            folder = self.service.files().create(
                body=file_metadata, 
                fields='id, webViewLink',
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"📁 Pasta corporativa criada: {folder_name} (ID: {folder.get('id')})")
            return folder.get('id'), folder.get('webViewLink')
        except Exception as e:
            logger.error(f"❌ Erro na criação de pasta em Drive Compartilhado: {e}")
            return None, None

    def upload_arquivo(self, file_path, folder_id):
        """
        Sobe um arquivo (Ficha de Cadastro) para a pasta do Deal.
        Garante a persistência em volumes compartilhados.
        """
        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='application/pdf')
        try:
            # CRITICAL: supportsAllDrives=True deve ser usado no upload também
            file = self.service.files().create(
                body=file_metadata, 
                media_body=media, 
                fields='id',
                supportsAllDrives=True
            ).execute()
            
            logger.info(f"📄 Documento formalizado no Drive: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            logger.error(f"❌ Erro no upload transacional: {e}")
            return None

    # --- NOVOS MÉTODOS PARA TAREFA DE AUDITORIA REAL ---

    def listar_subpastas(self, parent_id):
        """
        Lista todas as pastas dentro de um diretório pai específico.
        Fundamental para varrer a raiz da Samba Export e encontrar Commodities.
        """
        query = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        try:
            results = self.service.files().list(
                q=query, 
                fields="files(id, name)", 
                supportsAllDrives=True, 
                includeItemsFromAllDrives=True,
                pageSize=1000
            ).execute()
            return results.get('files', [])
        except Exception as e:
            logger.error(f"❌ Erro ao listar subpastas no nó {parent_id}: {e}")
            return []

    def verificar_ficha_existente(self, folder_id):
        """
        Verifica se a pasta do negócio já contém o PDF da Ficha de Cadastro.
        Evita duplicação de documentos durante a auditoria.
        """
        query = f"'{folder_id}' in parents and name contains 'FICHA_CADASTRO' and trashed = false"
        try:
            results = self.service.files().list(
                q=query, 
                fields="files(id)", 
                supportsAllDrives=True, 
                includeItemsFromAllDrives=True
            ).execute()
            return len(results.get('files', [])) > 0
        except Exception as e:
            logger.error(f"❌ Erro ao verificar existência de ficha na pasta {folder_id}: {e}")
            return False

    def ler_contexto_da_pasta(self, folder_id):
        """
        Lê a lista de arquivos de uma pasta para fornecer contexto à IA.
        Permite que o Agente entenda o negócio mesmo sem mensagens de WhatsApp.
        """
        query = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
        try:
            results = self.service.files().list(
                q=query, 
                fields="files(id, name, mimeType)", 
                supportsAllDrives=True, 
                includeItemsFromAllDrives=True
            ).execute()
            return results.get('files', [])
        except Exception as e:
            logger.error(f"❌ Erro ao ler contexto da pasta {folder_id}: {e}")
            return []