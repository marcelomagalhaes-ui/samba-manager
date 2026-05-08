"""
services/document_parser.py
===========================
Motor de Extração de Texto para Anexos do WhatsApp.
Abre PDFs (ICPOs, LOIs, SCOs) e Arquivos Word (SPAs, FCOs) que caem na Drop-Zone,
extrai o texto bruto e entrega para o Agente Extrator analisar.
"""
import io
import logging
import PyPDF2
import docx

logger = logging.getLogger(__name__)

class DocumentParser:
    
    @staticmethod
    def extract_from_pdf(file_bytes: bytes) -> str:
        """Lê o conteúdo de um arquivo PDF e retorna o texto."""
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(file_bytes))
            text = []
            for page in reader.pages:
                extracted = page.extract_text()
                if extracted:
                    text.append(extracted)
            return "\n".join(text).strip()
        except Exception as e:
            logger.error(f"❌ Erro ao ler PDF: {e}")
            return ""

    @staticmethod
    def extract_from_docx(file_bytes: bytes) -> str:
        """Lê o conteúdo de um arquivo Word (.docx) e retorna o texto."""
        try:
            doc = docx.Document(io.BytesIO(file_bytes))
            text = [para.text for para in doc.paragraphs if para.text.strip()]
            return "\n".join(text).strip()
        except Exception as e:
            logger.error(f"❌ Erro ao ler DOCX: {e}")
            return ""

    @classmethod
    def process_file(cls, file_bytes: bytes, mime_type: str, filename: str = "") -> str:
        """
        Roteador Inteligente: Olha para o tipo do arquivo e aciona o extrator correto.
        Retorna o texto extraído pronto para o Prompt do Gemini.
        """
        logger.info(f"📄 Tentando extrair texto do anexo: {filename}...")
        
        texto_extraido = ""
        
        if 'pdf' in mime_type.lower() or filename.lower().endswith('.pdf'):
            texto_extraido = cls.extract_from_pdf(file_bytes)
        elif 'word' in mime_type.lower() or 'officedocument' in mime_type.lower() or filename.lower().endswith('.docx'):
            texto_extraido = cls.extract_from_docx(file_bytes)
        else:
            logger.warning(f"⚠️ Formato não suportado para extração direta de texto: {filename}")
            return f"[ARQUIVO ANEXADO: {filename} - FORMATO NÃO SUPORTADO PARA LEITURA DE TEXTO]"
            
        if texto_extraido:
            # Adicionamos um cabeçalho para a IA saber que isso veio de um documento
            return f"\n--- INÍCIO DO DOCUMENTO ANEXADO ({filename}) ---\n{texto_extraido}\n--- FIM DO DOCUMENTO ---\n"
        else:
            return f"[ARQUIVO ANEXADO: {filename} - NÃO FOI POSSÍVEL EXTRAIR O TEXTO (Pode ser uma imagem/scan)]"

document_parser = DocumentParser()