"""
services/knowledge_ingestion.py
===============================
Injeta a "Apostila Samba Export" no banco de dados local (SQLite).
Permite que os Agentes de IA consultem as regras corporativas OFFLINE (Custo Zero),
sem precisar enviar PDFs gigantescos para a API do Gemini/Claude.
"""
import sys
from pathlib import Path
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import declarative_base, sessionmaker

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import get_engine

Base = declarative_base()

class CorporateKnowledge(Base):
    __tablename__ = 'corporate_knowledge'
    id = Column(Integer, primary_key=True)
    categoria = Column(String(50), index=True) # Ex: SWIFT, INCOTERM, SPA, COMPLIANCE
    topico = Column(String(100))               # Ex: MT760, CIF, Performance Bond
    regras_base = Column(Text)                 # O conteúdo extraído da Apostila

engine = get_engine()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def popular_cerebro_offline():
    # Limpa a base antiga para evitar duplicatas
    session.query(CorporateKnowledge).delete()
    
    conhecimentos = [
        # 1. REGRAS DO SISTEMA SWIFT E GARANTIAS (Capítulo 4 e 6)
        CorporateKnowledge(
            categoria="SWIFT", topico="MT760 - SBLC",
            regras_base="Standby Letter of Credit (SBLC). Regulada pela ISP98. Mensagem SWIFT MT760. Deve ser incondicional, irrevogável e pagável 'on first demand'. Emitida após assinatura do SPA para garantir o pagamento."
        ),
        CorporateKnowledge(
            categoria="SWIFT", topico="MT700 - DLC",
            regras_base="Documentary Letter of Credit. Regulada pela UCP 600. Pagamento condicionado à apresentação de documentos de embarque rigorosos (B/L, SGS, Invoice)."
        ),
        CorporateKnowledge(
            categoria="SWIFT", topico="MT799 - Free Format",
            regras_base="Mensagem livre entre bancos correspondentes. Usada OBRIGATORIAMENTE antes do MT760 para verificação de autenticidade (pré-aviso) e prevenção de fraudes."
        ),
        CorporateKnowledge(
            categoria="GARANTIA", topico="Performance Bond (PB)",
            regras_base="Garantia de performance emitida pelo vendedor via MT760 a favor do comprador. Varia de 2% a 10% do contrato. Executável 'on first demand' em caso de falha de entrega."
        ),

        # 2. INCOTERMS 2020 E TRANSFERÊNCIA DE RISCO (Capítulo 3)
        CorporateKnowledge(
            categoria="INCOTERM", topico="FOB - Free On Board",
            regras_base="Vendedor entrega a bordo do navio no porto de embarque. RISCO: transfere-se no embarque. CUSTO: frete e seguro por conta do comprador. Ideal para soja e minerais."
        ),
        CorporateKnowledge(
            categoria="INCOTERM", topico="CIF - Cost, Insurance and Freight",
            regras_base="Vendedor paga frete e seguro até o porto de destino. RISCO: transfere-se NO EMBARQUE no porto de origem. Se houver sinistro no mar, o comprador aciona a apólice."
        ),

        # 3. FLUXO DOCUMENTAL E COMPLIANCE (Capítulos 5 e 7)
        CorporateKnowledge(
            categoria="FLUXO", topico="Ordem de Operação",
            regras_base="1. ICPO + RWA (Intenção e fundo). 2. FCO (Oferta do vendedor). 3. SPA (Contrato assinado). 4. SBLC MT760 (Garantia do comprador). 5. PB 2% (Garantia do vendedor). 6. POP e SGS (Inspeção). 7. B/L (Embarque). 8. MT103 (Liquidação)."
        ),
        CorporateKnowledge(
            categoria="COMPLIANCE", topico="Due Diligence & POP",
            regras_base="Sempre exigir Q&Q Report (SGS, Bureau Veritas), Certificado de Origem (COO) e Product Passport. Validar compliance com regras AML (Lavagem de Dinheiro) e KYC. Risco de 'Ghost Cargo' (cargas falsas) deve ser mitigado."
        ),

        # 4. CLÁUSULAS CHAVE PARA O AGENTE DOCUMENTAL REDIGIR (Capítulo 5.3)
        CorporateKnowledge(
            categoria="SPA_CLAUSES", topico="Cláusula de Arbitragem",
            regras_base="Arbitragem internacional deve seguir a ICC (Paris) ou LCIA (Londres). A cláusula deve indicar: 'All disputes shall be finally settled under the Rules of Arbitration of the ICC. The seat of arbitration shall be London. Language: English.'"
        )
    ]
    
    session.add_all(conhecimentos)
    session.commit()
    print("🧠 Apostila Samba Export injetada no Banco de Dados com sucesso! IA Offline Pronta.")

if __name__ == "__main__":
    popular_cerebro_offline()