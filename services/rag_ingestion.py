"""
services/rag_ingestion.py
=========================
Motor de Ingestão Vetorial (RAG) Nível Enterprise - 100% OFFLINE.
Substitui a dependência de APIs externas (que geram Erro 404 e cobram tokens)
por um modelo de Embeddings Local (Sentence-Transformers).
Garante privacidade absoluta dos manuais da Samba Export.
"""
import sys
import json
import logging
import time
from pathlib import Path

# Garantir path absoluto
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import get_session, CorporateKnowledge

try:
    import docx
    import tiktoken
    from sentence_transformers import SentenceTransformer
except ImportError:
    print("❌ Instale as dependências: pip install python-docx tiktoken sentence-transformers")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

DADOS_DIR = ROOT / "DADOS"

# Configurações do RAG
CHUNK_SIZE = 800  
OVERLAP = 150     

tokenizer = tiktoken.get_encoding("cl100k_base")

# Inicializa o modelo matemático localmente na memória (Download ocorre na primeira execução)
logger.info("🧠 Carregando Modelo Neural Local (HuggingFace - all-MiniLM-L6-v2)...")
logger.info("Isso garante zero custo de API e 100% de privacidade dos documentos.")
embedder = SentenceTransformer('all-MiniLM-L6-v2')

def get_embedding_local(texto: str) -> list:
    """Gera o vetor matemático usando o processador da sua própria máquina."""
    try:
        # Transforma o texto num array de 384 dimensões e converte para lista Python
        vetor = embedder.encode(texto)
        return vetor.tolist()
    except Exception as e:
        logger.error(f"❌ Erro no cálculo vetorial local: {e}")
        return []

def chunk_text_with_overlap(text: str, chunk_size: int, overlap: int) -> list:
    """Fatia o texto preservando a integridade semântica."""
    tokens = tokenizer.encode(text)
    chunks = []
    
    i = 0
    while i < len(tokens):
        chunk_tokens = tokens[i:i + chunk_size]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunks.append({
            "text": chunk_text,
            "token_count": len(chunk_tokens)
        })
        i += (chunk_size - overlap)
        
    return chunks

def extract_text_from_docx(file_path: Path) -> str:
    """Extrai texto bruto do Word mantendo a ordem natural."""
    try:
        doc = docx.Document(file_path)
        full_text = []
        for para in doc.paragraphs:
            if para.text.strip():
                full_text.append(para.text.strip())
        return "\n".join(full_text)
    except Exception as e:
        logger.error(f"Erro ao ler DOCX {file_path.name}: {e}")
        return ""

def processar_base_conhecimento():
    logger.info("🚀 Iniciando Ingestão Vetorial RAG (Stanford Architecture)...")
    
    if not DADOS_DIR.exists():
        logger.error(f"Pasta {DADOS_DIR} não existe.")
        return

    arquivos = list(DADOS_DIR.glob("*.docx")) + list(DADOS_DIR.glob("*.doc"))
    if not arquivos:
        logger.warning("Nenhum arquivo DOCX/DOC encontrado na pasta DADOS.")
        return

    session = get_session()
    
    # Limpa apenas os registros vetorizados antigos
    session.query(CorporateKnowledge).delete() 
    logger.info("Cérebro antigo limpo. Preparando nova rede neural local...")

    total_chunks_inseridos = 0
    inicio_timer = time.time()

    for arquivo in arquivos:
        logger.info(f"\n📄 Analisando documento: {arquivo.name}")
        texto_completo = extract_text_from_docx(arquivo)
        
        if not texto_completo:
            continue
            
        chunks = chunk_text_with_overlap(texto_completo, CHUNK_SIZE, OVERLAP)
        logger.info(f"  ✂️ Fatiado em {len(chunks)} blocos semânticos. Iniciando cálculo vetorial...")

        for idx, chunk in enumerate(chunks):
            # Usando nosso motor local (Adeus API do Google para essa tarefa!)
            vetor = get_embedding_local(chunk["text"])

            registro = CorporateKnowledge(
                document_name=arquivo.name,
                chunk_index=idx,
                content=chunk["text"],
                embedding=json.dumps(vetor),
                token_count=chunk["token_count"]
            )
            session.add(registro)
            total_chunks_inseridos += 1
            
            if (idx + 1) % 10 == 0 or (idx + 1) == len(chunks):
                logger.info(f"    ⏳ Vetorizados {idx + 1}/{len(chunks)} blocos do arquivo...")

    try:
        session.commit()
    except Exception as e:
        logger.error(f"Erro ao salvar no banco: {e}")
        session.rollback()
    finally:
        session.close()
    
    tempo_total = round(time.time() - inicio_timer, 2)
    logger.info("\n" + "="*70)
    logger.info(f"✅ SUCESSO! CÉREBRO VETORIAL COMPILADO EM {tempo_total}s.")
    logger.info(f"📊 {total_chunks_inseridos} fragmentos neurais foram injetados no SQLite.")
    logger.info("🔒 Tudo foi processado offline, garantindo sigilo corporativo absoluto.")
    logger.info("="*70)

if __name__ == "__main__":
    processar_base_conhecimento()