"""
services/google_drive.py
========================
Módulo de integração com o Google Drive da Samba Export.
Permite que os Agentes de IA leiam templates, contratos e histórico
diretamente da pasta raiz corporativa para tomarem decisões embasadas.

Autenticação suportada (em ordem de prioridade):
  1. Service Account  → GOOGLE_SERVICE_ACCOUNT_FILE=config/service_account.json
     Mesma conta pode ser usada para Gemini via Vertex AI (se necessário).
     Requer que a pasta do Drive seja compartilhada com o e-mail da service account.
  2. OAuth2 (usuário) → config/credentials.json + config/token.json (fluxo atual)

Nota: A GEMINI_API_KEY (AI Studio) é separada das credenciais do Drive.
      Uma Service Account pode compartilhar o mesmo JSON para ambos apenas
      via Vertex AI — para uso simples, mantenha API Key e Drive separados.
"""
import os
import io
import logging
from pathlib import Path
from typing import List, Dict, Optional

from dotenv import load_dotenv

# --- CORREÇÃO 1: Importar o MediaIoBaseDownload ---
from googleapiclient.http import MediaIoBaseDownload

load_dotenv()

# Força o logger a mostrar as mensagens no terminal para não voarmos às cegas
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# O ID da pasta raiz da Samba Export
SAMBA_ROOT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "0AOllQoxhuNj4Uk9PVA")

# Escopos necessários
# drive        → leitura/escrita no Drive e Shared Drives corporativos
# gmail.send   → envio de email como agente@sambaexport.com.br (notificações internas)
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/gmail.send',
]


class DriveManager:
    """Gestor de ficheiros do Google Drive para os Agentes."""

    def __init__(self):
        self.creds = None
        self.service = None
        self._authenticate()

    def _authenticate(self):
        """
        Autentica usando (em ordem de prioridade):
          1. Service Account via arquivo (config/service_account.json)
          2. OAuth2 via token.json no disco (ambiente local)
          3. OAuth2 via GOOGLE_TOKEN_JSON na env (Streamlit Cloud secrets)
        """
        # ── Importações lazy para não quebrar se bibliotecas faltarem ────────
        try:
            from googleapiclient.discovery import build
        except ImportError:
            logger.warning("google-api-python-client não instalado. Drive indisponível.")
            return

        sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json")
        sa_path = Path(sa_file)

        # ── Opção 1: Service Account (arquivo) ────────────────────────────────
        if sa_path.exists():
            try:
                from google.oauth2 import service_account
                self.creds = service_account.Credentials.from_service_account_file(
                    str(sa_path), scopes=SCOPES
                )
                self.service = build('drive', 'v3', credentials=self.creds)
                logger.info("✓ Google Drive conectado via Service Account.")
                return
            except Exception as exc:
                logger.warning("Falha ao autenticar via Service Account: %s", exc)

        # ── Opção 2 & 3: OAuth2 (arquivo local ou env var do Streamlit Cloud) ─
        try:
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
            from google.auth.transport.requests import Request
            import json as _json

            token_path = Path("config/token.json")
            credentials_path = Path(
                os.getenv("GOOGLE_CREDENTIALS_FILE", "config/credentials.json")
            )

            # Opção 2: arquivo token.json local
            if token_path.exists():
                self.creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

            # Opção 3: variável de ambiente GOOGLE_TOKEN_JSON (Streamlit Cloud)
            if not self.creds:
                token_json_str = os.getenv("GOOGLE_TOKEN_JSON", "")
                if token_json_str:
                    try:
                        self.creds = Credentials.from_authorized_user_info(
                            _json.loads(token_json_str), SCOPES
                        )
                        logger.info("Token OAuth2 carregado via GOOGLE_TOKEN_JSON (env).")
                    except Exception as exc:
                        logger.warning("Falha ao parsear GOOGLE_TOKEN_JSON: %s", exc)

            if not self.creds or not self.creds.valid:
                if self.creds and self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
                else:
                    if not credentials_path.exists():
                        logger.warning(
                            "Nenhuma credencial Google encontrada. "
                            "Forneça config/service_account.json, config/token.json "
                            "ou a secret GOOGLE_TOKEN_JSON."
                        )
                        return
                    flow = InstalledAppFlow.from_client_secrets_file(
                        str(credentials_path), SCOPES
                    )
                    logger.info("=====================================================")
                    logger.info("⚠️ ATENÇÃO: Autenticação Manual Requerida")
                    logger.info("Copie o link abaixo e cole no CHROME para autorizar:")
                    logger.info("=====================================================")
                    self.creds = flow.run_local_server(port=0, open_browser=False)

                # Salva token apenas se estivermos em ambiente local (arquivo existe)
                if token_path.parent.exists() and not os.getenv("GOOGLE_TOKEN_JSON"):
                    with open(token_path, 'w') as token:
                        token.write(self.creds.to_json())

            self.service = build('drive', 'v3', credentials=self.creds)
            logger.info("✓ Google Drive conectado via OAuth2.")

        except Exception as exc:
            logger.warning("Falha ao autenticar Google Drive: %s", exc)

    def listar_documentos_conhecimento(self, folder_id: str = SAMBA_ROOT_FOLDER_ID) -> List[Dict]:
        """
        Mapeia todos os documentos na pasta (ignorando pastas) para criar o 'Índice de Conhecimento' dos agentes.
        """
        if not self.service:
            return []
            
        try:
            items = []
            try:
                # TENTATIVA 1: Busca total assumindo que folder_id é um Shared Drive
                logger.info("A tentar ler como Drive Compartilhado...")
                query_drive = f"mimeType != 'application/vnd.google-apps.folder' and trashed = false"
                results = self.service.files().list(
                    q=query_drive,
                    pageSize=100,
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=folder_id # Diz à API que este ID é o próprio Drive
                ).execute()
                items = results.get('files', [])
                
            except Exception as e1:
                logger.warning(f"Não é raiz de Shared Drive ({e1}). A tentar como pasta normal...")
                
                # TENTATIVA 2: Busca assumindo que folder_id é apenas uma subpasta
                query_folder = f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false"
                results = self.service.files().list(
                    q=query_folder,
                    pageSize=100,
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="allDrives"
                ).execute()
                items = results.get('files', [])

            logger.info(f"Encontrados {len(items)} documentos de conhecimento na pasta.")
            return items
            
        except Exception as e:
            logger.error(f"Erro fatal ao listar ficheiros do Drive: {e}")
            return []

    def ler_conteudo_documento(self, file_id: str, mime_type: str) -> Optional[str]:
        """
        Lê o conteúdo de um ficheiro específico para injetar no cérebro do Claude/Gemini.
        Suporta Google Docs e Google Sheets.
        """
        if not self.service:
            return None
            
        try:
            # Se for um Google Doc, exporta para texto simples
            if mime_type == 'application/vnd.google-apps.document':
                request = self.service.files().export_media(fileId=file_id, mimeType='text/plain')
            # Se for um ficheiro normal (.txt, .csv)
            elif mime_type in ['text/plain', 'text/csv', 'application/pdf']:
                request = self.service.files().get_media(
                    fileId=file_id,
                    supportsAllDrives=True # GARANTE A LEITURA EM DRIVES CORPORATIVOS
                )
            else:
                logger.info(f"Formato não suportado para leitura direta de IA: {mime_type}")
                return None

            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                
            return fh.getvalue().decode('utf-8')
            
        except Exception as e:
            logger.error(f"Erro ao ler conteúdo do ficheiro {file_id}: {e}")
            return None

    # --- NOVO MÉTODO: DOWNLOAD BINÁRIO PARA PDFs e DOCX ---
    def download_file_bytes(self, file_id: str) -> Optional[bytes]:
        """
        Faz o download de um arquivo do Drive em formato binário (bytes).
        Crucial para o Document Parser ler PDFs, Imagens e Arquivos Word.
        """
        if not self.service:
            return None
            
        try:
            request = self.service.files().get_media(
                fileId=file_id,
                supportsAllDrives=True
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            
            return fh.getvalue()
        except Exception as e:
            logger.error(f"Erro ao baixar bytes do arquivo {file_id}: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════════
    # LOI Generator — helpers de busca/upload em pastas específicas
    # ═══════════════════════════════════════════════════════════════════

    # MIME types relevantes
    MIME_GOOGLE_DOC = "application/vnd.google-apps.document"
    MIME_DOCX = (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )

    def find_file_by_name(
        self,
        filename: str,
        folder_id: str,
        ignore_underscore_prefix: bool = True,
    ) -> Optional[Dict]:
        """
        Procura um arquivo por nome dentro de uma pasta — TOLERANTE a extensão.

        Templates podem estar salvos como Google Docs nativos (sem extensão)
        OU como .docx. Este método tenta múltiplas variações:
          1. Nome exato (ex.: "MODEL-LOI-SOY-SE.docx")
          2. Nome sem extensão (ex.: "MODEL-LOI-SOY-SE")    ← Google Doc nativo
          3. Nome com .docx adicionado (caso o input seja stem)

        Args:
            filename: nome do template (com ou sem extensão)
            folder_id: ID da pasta-pai
            ignore_underscore_prefix: se True, ignora arquivos iniciados com "_"

        Returns:
            dict {id, name, mimeType} ou None se não encontrado
        """
        if not self.service:
            return None

        # Gera lista de candidatos. PRIORIDADE: Google Doc nativo (sem extensão)
        # > .docx, porque o user mantém os Google Docs como fonte editável.
        if filename.lower().endswith(".docx"):
            stem = filename[:-5]
            candidates = [stem, filename]              # gdoc primeiro, depois .docx
        else:
            candidates = [filename, filename + ".docx"]   # idem

        for cand in candidates:
            try:
                safe = cand.replace("'", "\\'")
                q = (
                    f"'{folder_id}' in parents "
                    f"and name = '{safe}' "
                    f"and trashed = false"
                )
                results = self.service.files().list(
                    q=q,
                    fields="files(id, name, mimeType)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="allDrives",
                    pageSize=10,
                ).execute()
                files = results.get("files", [])
                if ignore_underscore_prefix:
                    files = [f for f in files if not f["name"].startswith("_")]
                if files:
                    logger.info(f"Template Drive: '{cand}' → {files[0]['mimeType']}")
                    return files[0]
            except Exception as exc:
                logger.error(f"find_file_by_name falhou ({cand}): {exc}")
        return None

    def find_file_by_prefix(
        self,
        prefix: str,
        folder_id: str,
        ignore_underscore_prefix: bool = True,
    ) -> Optional[Dict]:
        """
        Procura arquivo cujo nome COMEÇA com *prefix* dentro de uma pasta.
        Usa o operador `name contains` da Drive API como fallback tolerante.

        Returns:
            dict {id, name, mimeType} do primeiro match, ou None.
        """
        if not self.service:
            return None
        try:
            safe = prefix.replace("'", "\\'")
            q = (
                f"'{folder_id}' in parents "
                f"and name contains '{safe}' "
                f"and trashed = false"
            )
            results = self.service.files().list(
                q=q,
                fields="files(id, name, mimeType)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="allDrives",
                pageSize=20,
            ).execute()
            files = results.get("files", [])
            if ignore_underscore_prefix:
                files = [f for f in files if not f["name"].startswith("_")]
            if files:
                logger.info(
                    f"find_file_by_prefix '{prefix}' → {files[0]['name']} "
                    f"({files[0]['mimeType']})"
                )
                return files[0]
        except Exception as exc:
            logger.error(f"find_file_by_prefix falhou ({prefix}): {exc}")
        return None

    def fetch_as_docx_bytes(self, file_meta: Dict) -> Optional[bytes]:
        """
        Retorna o conteúdo de um arquivo do Drive como bytes .docx.

        Trata 2 cenários:
          - Google Doc nativo (mimeType=application/vnd.google-apps.document):
            usa export_media para converter em .docx.
          - Arquivo .docx binário: usa get_media (download direto).

        Args:
            file_meta: dict retornado por find_file_by_name (precisa id, mimeType)
        """
        if not self.service:
            return None
        try:
            file_id = file_meta["id"]
            mime    = file_meta.get("mimeType", "")
            if mime == self.MIME_GOOGLE_DOC:
                request = self.service.files().export_media(
                    fileId=file_id, mimeType=self.MIME_DOCX,
                )
            else:
                request = self.service.files().get_media(
                    fileId=file_id, supportsAllDrives=True,
                )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return fh.getvalue()
        except Exception as exc:
            logger.error(f"fetch_as_docx_bytes falhou ({file_meta.get('name')}): {exc}")
            return None

    def export_gdoc_as_pdf_bytes(self, file_id: str) -> Optional[bytes]:
        """Exporta um Google Doc existente como bytes PDF."""
        if not self.service:
            return None
        try:
            request = self.service.files().export_media(
                fileId=file_id, mimeType="application/pdf"
            )
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return fh.getvalue()
        except Exception as exc:
            logger.error(f"export_gdoc_as_pdf_bytes falhou ({file_id}): {exc}")
            return None

    def delete_file(self, file_id: str) -> bool:
        """Move arquivo para a lixeira do Drive (compatível com Shared Drives)."""
        if not self.service:
            return False
        try:
            self.service.files().update(
                fileId=file_id,
                body={"trashed": True},
                supportsAllDrives=True,
            ).execute()
            logger.info(f"✓ Arquivo Drive movido para lixeira: {file_id}")
            return True
        except Exception as exc:
            logger.warning(f"delete_file (trash) falhou ({file_id}): {exc}")
            return False

    def upload_file_bytes(
        self,
        filename: str,
        content: bytes,
        folder_id: str,
        mime_type: str = MIME_DOCX,
        save_as_google_doc: bool = True,
    ) -> Optional[Dict]:
        """
        Upload de bytes para uma pasta específica do Drive.

        Args:
            filename: nome do arquivo
            content: bytes do .docx renderizado
            folder_id: ID da pasta-destino
            mime_type: MIME do CONTEÚDO sendo enviado (default: docx)
            save_as_google_doc: se True (default), pede ao Drive para CONVERTER
                                o .docx em Google Doc nativo durante o upload.
                                Mantém consistência com os templates de origem.

        Returns:
            dict {id, name, webViewLink} ou None em erro
        """
        if not self.service:
            logger.error("upload_file_bytes: serviço Drive não inicializado")
            return None
        try:
            from googleapiclient.http import MediaIoBaseUpload
            media = MediaIoBaseUpload(
                io.BytesIO(content), mimetype=mime_type, resumable=False,
            )
            # Se save_as_google_doc, define o mimeType DESTINO no metadata.
            # O Drive automaticamente converte o .docx enviado em Google Doc.
            target_name = filename
            if save_as_google_doc and target_name.lower().endswith(".docx"):
                target_name = target_name[:-5]    # remove .docx do nome final
            metadata = {"name": target_name, "parents": [folder_id]}
            if save_as_google_doc:
                metadata["mimeType"] = self.MIME_GOOGLE_DOC

            created = self.service.files().create(
                body=metadata,
                media_body=media,
                fields="id, name, webViewLink, mimeType",
                supportsAllDrives=True,
            ).execute()
            logger.info(
                f"✓ Upload Drive: {target_name} → {created.get('id')} "
                f"(mime={created.get('mimeType')})"
            )
            return created
        except Exception as exc:
            logger.error(f"upload_file_bytes falhou ({filename}): {exc}")
            return None

    def mapear_estrutura_pastas(self, folder_id: str = SAMBA_ROOT_FOLDER_ID, caminho_atual: str = "Samba Export") -> List[Dict]:
        """
        Mapeia recursivamente todas as pastas e arquivos, criando uma árvore de conhecimento que preserva o contexto geográfico.
        """
        if not self.service:
            return []
            
        estrutura = []
        try:
            items = []
            # Diferente da listagem de conhecimento, aqui buscamos TUDO, incluindo pastas
            try:
                # TENTATIVA 1: Busca como Shared Drive (para a raiz)
                # NOTA: Quando listamos o conteúdo de um drive inteiro, não usamos 'in parents'
                query_drive = f"trashed = false"
                results = self.service.files().list(
                    q=query_drive,
                    pageSize=1000,
                    fields="files(id, name, mimeType, parents)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    driveId=folder_id
                ).execute()
                
                todas_files_drive = results.get('files', [])
                
                # No Drive Corporativo, a estrutura é achatada. Vamos construir a árvore em memória.
                # Para simplificar e manter a interface recursiva padrão: 
                # Se for Drive raiz, forçamos o fallback para a "Tentativa 2" que lida bem com hierarquia.
                raise Exception("Forçando fallback para busca hierárquica.")

            except Exception:
                # TENTATIVA 2: Busca hierárquica (Nó por Nó)
                query_folder = f"'{folder_id}' in parents and trashed = false"
                results = self.service.files().list(
                    q=query_folder,
                    pageSize=1000,
                    fields="files(id, name, mimeType)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="allDrives"
                ).execute()
                items = results.get('files', [])

            for item in items:
                is_folder = item['mimeType'] == 'application/vnd.google-apps.folder'
                novo_caminho = f"{caminho_atual} / {item['name']}"
                
                item_info = {
                    'id': item['id'],
                    'name': item['name'],
                    'type': 'pasta' if is_folder else 'arquivo',
                    'path': novo_caminho
                }
                estrutura.append(item_info)
                
                # Recursão: Mergulha na subpasta
                if is_folder:
                    sub_itens = self.mapear_estrutura_pastas(
                        folder_id=item['id'], 
                        caminho_atual=novo_caminho
                    )
                    estrutura.extend(sub_itens)
                    
            return estrutura
            
        except Exception as e:
            logger.error(f"Erro ao mapear estrutura no nó {caminho_atual}: {e}")
            return []

# Instância global
drive_manager = DriveManager()