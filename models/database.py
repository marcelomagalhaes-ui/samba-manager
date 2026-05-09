"""
models/database.py
==================
Modelos de banco de dados — SQLAlchemy.
Arquitetura atualizada para suportar RAG (Cérebro Vetorial), Olho Biônico (PDFs),
e o Motor de Dados de Mercado (Físico + Bolsas + Macro).
"""
from datetime import datetime
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()


# ==============================================================
# MÓDULO 1: COMUNICAÇÃO E PIPELINE COMERCIAL (AGENTE EXTRATOR)
# ==============================================================

class Message(Base):
    """Mensagem capturada do WhatsApp."""
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    sender = Column(String(200), nullable=False, index=True)
    content = Column(Text)
    group_name = Column(String(200), index=True)
    is_media = Column(Boolean, default=False)
    is_system = Column(Boolean, default=False)
    
    # --- Campos para Olho Biônico (PDFs/DOCs) ---
    has_attachments = Column(Boolean, default=False)
    attachment_name = Column(String(255), nullable=True)
    attachment_mime_type = Column(String(100), nullable=True)
    attachment_data = Column(LargeBinary, nullable=True) # Salva os bytes do PDF para extração

    # Dados extraídos nativamente ou via regex simples
    commodity = Column(String(100), index=True)
    price = Column(Float)
    currency = Column(String(10))
    volume = Column(Float)
    volume_unit = Column(String(10))
    incoterm = Column(String(10))
    location = Column(String(200))
    has_quote = Column(Boolean, default=False, index=True)
    
    # Metadata
    processed_at = Column(DateTime, default=datetime.utcnow)


class Deal(Base):
    """Deal no pipeline comercial."""
    __tablename__ = "deals"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(300))
    
    # --- Inteligência Comercial Nível Sênior ---
    direcao = Column(String(10), default="UNKNOWN") # BID (Compra) ou ASK (Venda)
    commodity = Column(String(100))
    volume = Column(Float)
    volume_unit = Column(String(10))
    price = Column(Float)
    currency = Column(String(10))
    incoterm = Column(String(10))
    origin = Column(String(200))
    destination = Column(String(200))
    
    # Pipeline e Compliance
    stage = Column(String(50), default="Lead Capturado", index=True)
    risk_score = Column(Integer)
    instrumentalizacao = Column(String(50), default="NENHUMA") # LOI, ICPO, FCO, SPA
    due_diligence = Column(Boolean, default=False)
    alerta_grupo_interno = Column(Text, nullable=True) # Alerta da IA para os humanos
    assignee = Column(String(100))
    
    # Source
    source_group = Column(String(200))
    source_message_id = Column(Integer)
    source_sender = Column(String(200))
    
    # Dates
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    due_date = Column(DateTime)
    
    # Status
    status = Column(String(50), default="ativo")
    notes = Column(Text)

    # Controle de sincronização — evita append duplo na planilha mesmo sob race condition
    synced_to_sheets = Column(Integer, default=0)  # 0=pendente, 1=sincronizado


class FollowUp(Base):
    """Follow-up agendado."""
    __tablename__ = "followups"
    id = Column(Integer, primary_key=True, autoincrement=True)
    deal_id = Column(Integer, index=True)
    target_person = Column(String(200))
    target_group = Column(String(200))
    message = Column(Text)
    due_at = Column(DateTime, index=True)
    sent_at = Column(DateTime)
    response_received = Column(Boolean, default=False)
    response_content = Column(Text)
    status = Column(String(50), default="pendente")  # pendente, enviado, respondido, expirado
    created_at = Column(DateTime, default=datetime.utcnow)


class QuoteHistory(Base):
    """Histórico de cotações por commodity."""
    __tablename__ = "quote_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, index=True)
    commodity = Column(String(100), index=True)
    source = Column(String(100))  # whatsapp, esalq, canal_rural
    price = Column(Float)
    currency = Column(String(10))
    volume = Column(Float)
    incoterm = Column(String(10))
    location = Column(String(200))
    sender = Column(String(200))
    group_name = Column(String(200))


# ==============================================================
# MÓDULO 2: CONHECIMENTO E CÉREBRO CORPORATIVO (RAG)
# ==============================================================

class StrategicData(Base):
    """Planilhas e Dados de Logística/Preços do Google Drive (Injetados via Pandas)."""
    __tablename__ = 'strategic_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    sheet_name = Column(String(100), index=True) # Nome da aba (Ex: ACUCAR, FORNECEDORES)
    raw_json = Column(Text) # Linha da planilha em JSON para cruzamento do Agente Manager


class CorporateKnowledge(Base):
    """
    Cérebro Vetorial da Samba Export (Arquitetura RAG).
    Substitui a lógica simplista antiga por blocos semânticos e Embeddings matemáticos.
    """
    __tablename__ = 'corporate_knowledge'
    id = Column(Integer, primary_key=True, autoincrement=True)
    document_name = Column(String(200), index=True) # Ex: "APOSTILA_AÇÚCAR.docx"
    chunk_index = Column(Integer)                   # Ordem do bloco no texto
    content = Column(Text)                          # O texto do fragmento semântico
    embedding = Column(Text)                        # O Vetor Matemático gerado pelo modelo local (JSON Array)
    token_count = Column(Integer)                   # Controle de custo/peso (quantos tokens tem este bloco)


# ==============================================================
# MÓDULO 3: DADOS EXTERNOS (MERCADO FÍSICO, BOLSAS E MACRO)
# ==============================================================

class MarketSnapshot(Base):
    """Retrato dos indicadores macro e internacionais no momento."""
    __tablename__ = "market_snapshots"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)

    usd_brl = Column(Float, nullable=False)

    ice_sugar_usd_mt = Column(Float, nullable=False)
    cbot_soy_usd_mt = Column(Float, nullable=False)
    cbot_corn_usd_mt = Column(Float, nullable=False)

    diesel_s10 = Column(Float, nullable=False)
    bunker_vlsfo = Column(Float, nullable=False)
    daily_hire = Column(Float, nullable=False)

class PrecoFisicoRaw(Base):
    """Dados brutos coletados por scrapers em cooperativas e praças físicas."""
    __tablename__ = "tb_preco_fisico_raw"
    id = Column(Integer, primary_key=True, index=True)
    
    uf = Column(String(2), nullable=False, index=True)
    cidade = Column(String(100), nullable=False, index=True)
    produto = Column(String(50), nullable=False, index=True)
    preco_brl_ton = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    fonte = Column(String(100), nullable=False)

class BolsasBase(Base):
    """Histórico sincronizado das Bolsas Internacionais (CBOT/ICE)."""
    __tablename__ = "tb_bolsas_base"
    id = Column(Integer, primary_key=True, index=True)

    commodity = Column(String(50), nullable=False, index=True)
    contract = Column(String(50), nullable=False)
    
    price_raw = Column(Float, nullable=False)
    unit_original = Column(String(50), nullable=False)
    conversion_factor = Column(Float, nullable=False)
    
    price_usd_mt = Column(Float, nullable=False)
    
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    source_flag = Column(String(50), nullable=False)


# ==============================================================
# MÓDULO 4: CONVERSATIONAL HUB (memória de conversa + tool calling)
# ==============================================================

class DriveSyncState(Base):
    """
    Armazena estado de sincronização do pipeline Drive → RAG.

    Funciona como um key-value store persistente com dois tipos de chave:

      "changes_page_token"
          Valor: StartPageToken atual da Changes API do Drive.
          Atualizado no final de cada delta-scan bem-sucedido.
          Garante que cada execução processa apenas ficheiros novos/alterados.

      "file_hash:{drive_file_id}"
          Valor: md5Checksum (binários) ou modifiedTime ISO (Google Docs).
          Permite idempotência: o ficheiro só é reingerido se o hash mudou,
          nunca vetorizando o mesmo conteúdo duas vezes.

    Uso:
        from models.database import DriveSyncState, get_session
        s = get_session()
        row = s.query(DriveSyncState).filter_by(key="changes_page_token").first()
    """
    __tablename__ = "drive_sync_state"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    key        = Column(String(300), unique=True, nullable=False, index=True)
    value      = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# ==============================================================
# MÓDULO 5: COMPLIANCE DOCUMENTAL (AGENTE DOCUMENTAL)
# ==============================================================

class DocumentCompliance(Base):
    """
    Registro de auditoria documental pelo DocumentalAgent.
    Armazena o resultado de cada auditoria ICC/UCP600 por documento.
    Selo: VERDE / AMARELO / VERMELHO.
    """
    __tablename__ = "document_compliance"
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Associacao ao pipeline
    deal_id = Column(Integer, nullable=True, index=True)
    file_name = Column(String(300), nullable=False)
    file_path = Column(String(500), nullable=True)
    document_type = Column(String(20), nullable=False, index=True)  # LOI, ICPO, FCO, SPA, NCNDA, IMFPA
    commodity = Column(String(100), nullable=True)

    # Resultado da auditoria
    status = Column(String(20), nullable=False, index=True)  # VERDE, AMARELO, VERMELHO
    score = Column(Integer, nullable=False, default=0)       # 0-100
    missing_clauses_count = Column(Integer, default=0)
    spec_divergences_count = Column(Integer, default=0)
    critical_issues = Column(Integer, default=0)

    # Sumario e JSON completo
    summary = Column(Text, nullable=True)
    report_json = Column(Text, nullable=True)   # JSON com missing_clauses + spec_divergences

    # Metadata
    audited_at = Column(DateTime, default=datetime.utcnow, index=True)
    audited_by = Column(String(100), default="DocumentalAgent")


class ConversationHistory(Base):
    """
    Log estruturado de conversas para o Conversational Hub.

    Armazena turnos completos no estilo Chat Completions:
      - role  ∈ {"user", "assistant", "tool", "system"}
      - content    → texto do turno (nullable quando o assistant só chamou tools)
      - tool_calls → JSON serializado das chamadas pedidas pela LLM (nullable)
      - session_id → agrupa turnos da mesma sessão (chat/conversa)

    Indexes em `session_id` e `timestamp` permitem reconstruir a janela de
    contexto de uma sessão em ordem cronológica O(log n).
    """
    __tablename__ = "conversation_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), index=True, nullable=False)
    role = Column(String(20), nullable=False)
    content = Column(Text, nullable=True)
    tool_calls = Column(JSON, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True, nullable=False)


# ==============================================================
# MÓDULO 8: AGENTES ATIVOS — ATA DE REUNIÃO + APROVAÇÕES HUMANAS
# ==============================================================

class MeetingActionItem(Base):
    """
    Action item extraído automaticamente de áudio de reunião pelo
    task_process_voice_meeting_minutes (Gemini 1.5 Pro multimodal).

    Vinculado opcionalmente ao Message de origem (WhatsApp audio).
    """
    __tablename__ = "meeting_action_items"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    message_id   = Column(Integer, nullable=True, index=True)   # FK lógica para messages.id
    meeting_date = Column(DateTime, default=datetime.utcnow, index=True)
    responsible  = Column(String(200), nullable=True, index=True)
    action       = Column(Text, nullable=False)
    priority     = Column(String(20), default="media")   # critica | alta | media | baixa
    status       = Column(String(20), default="pendente", index=True)  # pendente | em_progresso | concluida
    due_date     = Column(DateTime, nullable=True)
    ata_snippet  = Column(Text, nullable=True)    # trecho da ata para contexto
    source_group = Column(String(200), nullable=True)  # grupo WPP de origem
    created_at   = Column(DateTime, default=datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PendingApproval(Base):
    """
    Fila de aprovação humana (Human-in-the-Loop).

    Ações de escrita (criar pasta, enviar e-mail, postar WhatsApp) ficam
    em status='pending' até um sócio aprovar via chat ou resposta WPP.
    """
    __tablename__ = "pending_approvals"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    action_type   = Column(String(100), nullable=False, index=True)  # create_folder | send_email | send_wpp | ...
    description   = Column(Text, nullable=True)   # descrição legível para o humano
    payload_json  = Column(Text, nullable=False)  # JSON com parâmetros da ação
    requested_by  = Column(String(100), nullable=True)  # agente/task que pediu
    session_id    = Column(String(100), nullable=True, index=True)  # sessão do Assistant que originou
    status        = Column(String(20), default="pending", index=True)  # pending | approved | rejected
    approved_by   = Column(String(100), nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)
    resolved_at   = Column(DateTime, nullable=True)


# ==============================================================
# CONFIGURAÇÃO DA ENGINE
# ==============================================================

import os as _os

def get_engine(url: str = None):
    if url is None:
        url = _os.getenv("DATABASE_URL")
    if url is None:
        try:
            import streamlit as _st
            url = _st.secrets.get("DATABASE_URL")
        except Exception:
            pass
    if not url:
        url = "sqlite:///data/samba_control.db"
    # Supabase porta 5432 resolve para IPv6 — Streamlit Cloud não suporta.
    # Substitui por porta 6543 (PgBouncer/IPv4) no mesmo host.
    if isinstance(url, str) and "supabase.co:5432" in url:
        url = url.replace(":5432/", ":6543/")
    return create_engine(url, echo=False)


def create_tables(url: str = None):
    engine = get_engine(url)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine=None, url: str = None):
    if engine is None:
        engine = get_engine(url)
    Session = sessionmaker(bind=engine)
    return Session()