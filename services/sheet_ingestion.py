"""
services/sheet_ingestion.py
===========================
Hub de Ingestão de Planilhas Estratégicas do Google Drive (Workbook Completo).
Baixa o arquivo inteiro de forma autenticada e mapeia TODAS as abas automaticamente.
Atua como "Fonte da Verdade" (Single Source of Truth) para evitar duplicidade de pedidos no WhatsApp.
"""
import sys
import pandas as pd
import requests
from io import BytesIO
from pathlib import Path
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base, sessionmaker

# Garantir path absoluto
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import get_engine
from services.google_drive import drive_manager

Base = declarative_base()

class StrategicData(Base):
    __tablename__ = 'strategic_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sheet_name = Column(String(100), index=True) # Ex: Aba Clientes, Aba Pedidos, Aba Logística
    raw_json = Column(Text) # A linha inteira convertida para a IA cruzar dados

engine = get_engine()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def injetar_workbook_completo(file_id: str):
    print(f"📥 Autenticando e extraindo o Workbook COMPLETO ID: {file_id}...")
    
    # 1. Pega as credenciais OAuth2
    creds = drive_manager.creds
    if not creds or not creds.valid:
        print("❌ Erro: Credenciais do Google Drive não encontradas.")
        return

    # 2. Exporta a planilha INTEIRA como Excel (.xlsx) para pegar todas as abas
    xlsx_export_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
    headers = {"Authorization": f"Bearer {creds.token}"}
    
    try:
        response = requests.get(xlsx_export_url, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ Falha no download. Código {response.status_code}: {response.text}")
            return
            
        print("⏳ Processando todas as abas do arquivo...")
        # 3. Lê todas as abas do arquivo Excel para a memória
        todas_as_abas = pd.read_excel(BytesIO(response.content), sheet_name=None)
        
        total_registros_gerais = 0
        
        # 4. Limpa toda a tabela para evitar lixo e dados antigos
        session.query(StrategicData).delete()
        
        # 5. Itera sobre cada aba encontrada na planilha
        for nome_aba, df in todas_as_abas.items():
            # Limpeza
            df = df.dropna(how='all')
            df = df.fillna("")
            linhas_aba = len(df)
            
            if linhas_aba == 0:
                continue
                
            registros = []
            for index, row in df.iterrows():
                linha_json = row.to_json(force_ascii=False)
                registro = StrategicData(
                    sheet_name=nome_aba,
                    raw_json=linha_json
                )
                registros.append(registro)
                
            session.add_all(registros)
            total_registros_gerais += len(registros)
            print(f"  -> Aba '{nome_aba}': {len(registros)} registros mapeados.")
            
        session.commit()
        print("="*60)
        print(f"✅ SUCESSO ABSOLUTO! {total_registros_gerais} registros de TODAS as abas foram injetados no banco.")
        print("🛡️ O SQLite agora atua como 'Single Source of Truth' para o Agente Extrator e Gerente Geral.")
        print("="*60)
        
    except ImportError:
        print("❌ Falta o pacote 'openpyxl'. Instale com: pip install openpyxl")
    except Exception as e:
        print(f"❌ Erro crítico ao processar o Workbook: {e}")

if __name__ == "__main__":
    # O link mestre que contém toda a inteligência comercial da Samba Export
    SHEET_ID = "1ZyLdyT6NlzM6vEuhin6PxBYLgcMtJ20Y"
    
    injetar_workbook_completo(file_id=SHEET_ID)