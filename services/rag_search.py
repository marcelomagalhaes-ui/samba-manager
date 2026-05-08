"""
services/rag_search.py
======================
O "Buscador Semântico" da Samba Export.
Dada uma dúvida ou contexto do WhatsApp, transforma em vetor e acha a 
regra exata na tabela CorporateKnowledge do SQLite usando Similaridade de Cosseno.
"""
import sys
import json
import logging
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import get_session, CorporateKnowledge

logger = logging.getLogger(__name__)

# Variável global para armazenar o modelo apenas após o primeiro uso (Lazy Loading)
_EMBEDDING_MODEL = None

def get_embedder():
    """
    Padrão Singleton / Lazy Loading.
    Carrega a IA pesada apenas quando for estritamente necessário (na hora da busca).
    Isso evita que o terminal congele ao iniciar a aplicação inteira.
    """
    global _EMBEDDING_MODEL
    if _EMBEDDING_MODEL is None:
        try:
            import warnings
            # Ignora avisos do HuggingFace/PyTorch para manter o log limpo
            warnings.filterwarnings("ignore") 
            
            from sentence_transformers import SentenceTransformer
            logger.info("⏳ Aquecendo o Motor Semântico RAG (Isso pode levar alguns segundos na primeira vez)...")
            # Modelo multilingual (PT/EN) — Sprint H.
            # Deve coincidir com MODEL_NAME em scripts/build_embeddings.py.
            _EMBEDDING_MODEL = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            logger.info("✅ Motor Semântico carregado com sucesso!")
        except ImportError:
            logger.error("Falta dependência. Rode: pip install sentence-transformers numpy")
            sys.exit(1)
            
    return _EMBEDDING_MODEL

def cosine_similarity(a, b):
    """Calcula a proximidade matemática entre duas ideias (Vetores). 1.0 é idêntico."""
    dot_product = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot_product / (norm_a * norm_b)

def buscar_contexto_corporativo(query: str, limite_resultados: int = 3) -> str:
    """
    Busca na base offline da Samba Export as regras que mais combinam com a query.
    Retorna um texto gigante com as regras prontas para injetar no Prompt do Gemini.
    """
    logger.info(f"🔎 RAG: Buscando contexto para -> '{query[:50]}...'")
    
    try:
        # 1. Transforma a dúvida do usuário/WhatsApp em matemática
        # O modelo só é invocado de fato aqui
        embedder = get_embedder()
        query_vector = embedder.encode(query).tolist()
    except Exception as e:
        logger.error(f"Erro ao vetorizar a query: {e}")
        return ""

    session = get_session()
    conhecimentos = session.query(CorporateKnowledge).all()
    
    if not conhecimentos:
        session.close()
        return ""

    resultados = []
    
    # 2. Faz a varredura contra os 225 fragmentos neurais
    for item in conhecimentos:
        try:
            item_vector = json.loads(item.embedding)
            if not item_vector:
                continue
                
            # Calcula a similaridade
            score = cosine_similarity(query_vector, item_vector)
            
            # Só aceitamos se houver o mínimo de correlação
            if score > 0.3:
                resultados.append({
                    "score": score,
                    "doc": item.document_name,
                    "texto": item.content
                })
        except Exception:
            continue
            
    session.close()

    # 3. Ordena do mais relevante para o menos relevante
    resultados.sort(key=lambda x: x["score"], reverse=True)
    top_resultados = resultados[:limite_resultados]
    
    if not top_resultados:
        return ""

    # 4. Formata o texto final para a IA
    contexto_injetado = "\n--- REGRAS DA SAMBA EXPORT (RETIRADAS DO DRIVE) ---\n"
    for idx, res in enumerate(top_resultados):
        contexto_injetado += f"Regra {idx+1} (Fonte: {res['doc']}):\n{res['texto']}\n\n"
    contexto_injetado += "--- FIM DAS REGRAS ---\n"
    
    logger.info(f"  ✓ RAG: Injetando {len(top_resultados)} regras relevantes na memória de curto prazo da IA.")
    
    return contexto_injetado