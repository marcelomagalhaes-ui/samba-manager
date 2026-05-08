"""
agents/extractor_agent.py
=========================
O Estruturador de Rotina (Exoskeleton Agent).
Responsável por atuar como back-office automatizado: lê o fluxo caótico de 
mensagens do WhatsApp, utiliza LLM (Gemini) para interpretar a semântica comercial 
(Preço, Volume, Incoterm) e estrutura a informação no banco de dados (Pipeline).

[ENTERPRISE UPGRADE]
- Normalização Multi-Deal: Extrai dezenas de negócios de um único "Broker Dump".
- Transações Atômicas: Commit isolado por mensagem para evitar perda de dados em lote.
- Data Quality Gate: Avalia saúde comercial e trava em "Qualificação" se faltar dado.
- Sincronização Bidirecional: Escreve no Google Sheets em tempo real.
- Escudo Anti-Duplicata: Bloqueia reentrada de negócios repetidos ou já declinados.
- Workspace Inteligente: Criação de pastas no Drive e geração de Ficha em PDF por negócio.
- Self-Healing (Auto-Cura): Audita negócios existentes e provisiona infraestrutura faltante.
"""

import sys
import logging
import hashlib
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any

# Garantir path absoluto
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import get_session, Message, Deal, QuoteHistory
from services.gemini_api import extract_quote_data, analyze_deal_risk

# --- INTEGRAÇÃO COM SERVIÇOS EXTERNOS ---
from services.google_sheets_sync import GoogleSheetsSync
from services.drive_service import DriveService
from services.pdf_service import PDFService

logging.basicConfig(level=logging.INFO, format='%(levelname)s: [%(name)s] %(message)s')
logger = logging.getLogger("ExtractorAgent")

# ============================================================================
# 1. KNOWLEDGE BASE (BASE DE CONHECIMENTO CORPORATIVO)
# ============================================================================

GRUPOS_FORNECEDORES = ["rokane", "conex", "fabricio", "gerson", "ze vasconcelos", "primex", "barreto", "mel", "usina"]
GRUPOS_COMPRADORES = ["bahov", "gwi", "dionathan", "maxin", "bicca", "dannyel", "bruno", "vilson", "ariel", "kent foods"]

ASSIGNEE_RULES = [
    (["soja", "milho", "trigo", "arroz", "feijão", "feijao", "sorgo", "farelo", "corn", "soy", "wheat"], "Leonardo"),
    (["açúcar", "acucar", "ic45", "icumsa", "etanol", "algodão", "algodao", "sugar", "vhp"], "Nivio"),
    (["café", "cafe", "cacau", "frango", "chicken", "boi", "beef", "porco", "pork", "oleo", "óleo", "oil", "tallow", "paw"], "Marcelo")
]

class ContextHeuristics:
    """Aplica regras de negócio para complementar a extração da IA."""
    
    @staticmethod
    def get_assignee(commodity: str) -> str:
        c = str(commodity).lower()
        for keywords, name in ASSIGNEE_RULES:
            if any(kw in c for kw in keywords):
                return name
        return "Leonardo" # Default
        
    @staticmethod
    def infer_direction(ai_direction: str, group_name: str, sender: str) -> str:
        if ai_direction and ai_direction.upper() in ["BID", "ASK"]:
            return ai_direction.upper()
            
        grp = str(group_name).lower() if group_name else ""
        if any(f in grp for f in GRUPOS_FORNECEDORES): return "ASK"
        if any(c in grp for c in GRUPOS_COMPRADORES): return "BID"
        
        return "UNKNOWN"

    @staticmethod
    def generate_deal_id_base(sender: str, commodity: str, timestamp: datetime, index: int = 0) -> str:
        """Gera Hash único base. O index previne colisão se a mesma msg tiver 2 negócios iguais."""
        s_clean = "".join(e for e in str(sender) if e.isalnum())[:8].upper()
        c_clean = "".join(e for e in str(commodity) if e.isalnum())[:8].upper()
        d_str = timestamp.strftime("%y%m%d") if timestamp else "000000"
        
        raw_str = f"{s_clean}_{c_clean}_{timestamp.isoformat() if timestamp else ''}_{index}"
        h = hashlib.md5(raw_str.encode()).hexdigest()[:4]
        
        return f"WPP_{d_str}_{s_clean[:4]}_{c_clean[:4]}_{h}"

# ============================================================================
# 2. DATA QUALITY GATE (VALIDAÇÃO DE COMPLETUDE)
# ============================================================================

class QualityController:
    """Avalia a saúde comercial da demanda extraída."""
    
    @staticmethod
    def check_completeness(deal_data: dict) -> list:
        """Verifica quais campos críticos estão faltando na negociação."""
        missing = []
        
        if not deal_data.get("commodity") or deal_data.get("commodity") == "Indefinida":
            missing.append("Produto/Especificação")
            
        if not deal_data.get("volume"):
            missing.append("Volume (MT/Sacas)")
            
        if not deal_data.get("incoterm"):
            missing.append("Incoterm (FOB/CIF/etc)")
            
        if deal_data.get("direcao") == "BID":
            if not deal_data.get("price"):
                missing.append("Target Price")
            if deal_data.get("incoterm") in ["CIF", "CFR"] and not deal_data.get("destination"):
                missing.append("Porto de Destino (Necessário para CIF)")
                
        return missing

# ============================================================================
# 3. MOTOR DE EXTRAÇÃO (AGENT)
# ============================================================================

class ExtractorAgent:
    def __init__(self):
        self.session = get_session()
        self.sheets_sync = GoogleSheetsSync()
        
        # Tenta inicializar serviços de Workspace (Drive e PDF)
        try:
            self.drive_service = DriveService()
            self.pdf_service = PDFService()
            self.workspace_enabled = True
        except Exception as e:
            logger.warning(f"Serviços de Workspace desativados (Drive/PDF falhou): {e}")
            self.workspace_enabled = False

    def process_pending_messages(self, limit: int = None):
        """Varre mensagens caóticas e popula o pipeline comercial, planilha e Drive."""
        logger.info("Iniciando Agente Estruturador (Exoskeleton)...")

        from sqlalchemy import or_
        query = self.session.query(Message).outerjoin(
            Deal, Message.id == Deal.source_message_id
        ).filter(
            Message.has_quote == True,
            Deal.id == None,
            or_(Message.price != None, Message.volume != None),
        ).order_by(Message.timestamp.asc())

        if limit:
            query = query.limit(limit)

        pending_messages = query.all()

        if not pending_messages:
            logger.info("Nenhuma mensagem caótica pendente de estruturação.")
            self.session.close()
            return

        logger.info(f"Delegando {len(pending_messages)} mensagens para interpretação semântica (LLM)...")
        deals_created = 0

        for msg in pending_messages:
            deals_created += self._process_one_message(msg)

        logger.info(f"Operação Back-Office concluída. {deals_created} negócios estruturados.")
        self.session.close()

    def process_single_message(self, msg_id: int) -> Dict[str, Any]:
        """
        Pipeline para UMA mensagem específica — driver Celery (webhook ao vivo).

        Idempotente: se a mensagem já tem Deal vinculado (`source_message_id`),
        retorna `skipped="already_processed"` sem reprocessar — o webhook pode
        re-entregar (Twilio retry) sem duplicar pipeline.
        """
        msg = self.session.query(Message).filter(Message.id == msg_id).first()
        if msg is None:
            logger.warning(f"process_single_message: msg_id={msg_id} não encontrada.")
            self.session.close()
            return {"deals_created": 0, "found": False}

        existing = self.session.query(Deal).filter(
            Deal.source_message_id == msg_id
        ).first()
        if existing is not None:
            logger.info(
                f"process_single_message: msg_id={msg_id} já tem Deal {existing.id} "
                f"(idempotente — sem reprocessar)."
            )
            self.session.close()
            return {"deals_created": 0, "skipped": "already_processed"}

        deals_created = self._process_one_message(msg)
        self.session.close()
        return {"deals_created": deals_created, "msg_id": msg_id}

    def _process_one_message(self, msg) -> int:
        """
        Estrutura UMA mensagem (extração + persistência + sync). Retorna o
        número de Deals criados a partir dela.

        Bloco protegido por try/commit/except/rollback — falha em uma mensagem
        NÃO contamina as demais (transação atômica por mensagem, preservada
        do design original do `process_pending_messages`).
        """
        logger.info(f"Analisando fluxo: {msg.sender} @ {msg.group_name}")
        deals_created = 0

        try:
            try:
                ai_data_raw = extract_quote_data(
                    message_text=msg.content,
                    sender=msg.sender,
                    group=msg.group_name
                )
            except Exception as e:
                logger.error(f"Falha de API na mensagem {msg.id}: {e}")
                return 0

            # --- NORMALIZAÇÃO MULTI-DEAL ---
            if isinstance(ai_data_raw, dict):
                extracted_deals = [ai_data_raw]
            elif isinstance(ai_data_raw, list):
                extracted_deals = ai_data_raw
            else:
                logger.warning(f"Formato de IA inesperado na msg {msg.id}. Ignorando.")
                return 0

            for idx, ai_data in enumerate(extracted_deals):
                if not isinstance(ai_data, dict):
                    continue

                if not ai_data.get("has_quote") or ai_data.get("confidence", 0.0) <= 0.6:
                    continue

                commodity = (ai_data.get("commodity") or msg.commodity or "Indefinida").title()
                volume = ai_data.get("volume") or msg.volume or 0
                unit = ai_data.get("volume_unit") or "MT"
                price = ai_data.get("price") or msg.price or 0
                currency = ai_data.get("currency") or msg.currency or "USD"
                incoterm = ai_data.get("incoterm")
                destination = ai_data.get("location")

                direcao = ContextHeuristics.infer_direction(ai_data.get("direction"), msg.group_name, msg.sender)
                assignee = ContextHeuristics.get_assignee(commodity)

                # ====================================================================
                # 🛡️ ESCUDO ANTI-DUPLICATA (MEMÓRIA INSTITUCIONAL)
                # ====================================================================
                # Buscamos qualquer rastro dessa negociação, inclusive na aba 'declinados'
                duplicata = self.session.query(Deal).filter(
                    Deal.source_sender == msg.sender,
                    Deal.commodity == commodity,
                    Deal.volume == volume,
                    Deal.price == price
                ).first()

                if duplicata:
                    status_msg = "já está no Pipeline" if duplicata.status == "ativo" else "já foi DECLINADO anteriormente"
                    logger.warning(f"  🔁 Bloqueio de Reentrada: Este negócio {status_msg} (ID: {duplicata.name}). Omitindo processamento.")
                    continue
                # ====================================================================

                # --- Geração de Nome e Provisionamento do Workspace (Drive + PDF) ---
                deal_base_id = ContextHeuristics.generate_deal_id_base(msg.sender, commodity, msg.timestamp, idx)
                vol_int = int(volume) if isinstance(volume, (int, float)) else volume
                deal_name = f"{deal_base_id} {vol_int}"

                folder_link = ""
                if self.workspace_enabled:
                    # 1. Cria Pasta no Drive
                    folder_id, folder_link = self.drive_service.criar_pasta_negocio(deal_name)

                    # 2. Prepara Ficha Cadastral PDF
                    deal_data_full = {
                        "name": deal_name,
                        "created_at": msg.timestamp or datetime.utcnow(),
                        "commodity": commodity,
                        "price": price,
                        "currency": currency,
                        "volume": volume,
                        "volume_unit": unit,
                        "incoterm": incoterm or "TBI",
                        "destination": destination or "TBI",
                        "source_group": msg.group_name,
                        "buyer": ai_data.get("buyer", "TBI"),
                        "spec": ai_data.get("spec", "TBI"),
                        "contract_type": ai_data.get("contract_type", "EXTENSIVO / SPOT"),
                        "commission": ai_data.get("commission", "TBI")
                    }

                    pdf_filename = f"FICHA_CADASTRO_{deal_base_id}.pdf"
                    temp_dir = os.path.join(ROOT, "temp")
                    os.makedirs(temp_dir, exist_ok=True)
                    pdf_path = os.path.join(temp_dir, pdf_filename)

                    try:
                        # 3. Gera arquivo e sobe para o Drive
                        self.pdf_service.gerar_ficha_pedido(deal_data_full, pdf_path)
                        if folder_id:
                            self.drive_service.upload_arquivo(pdf_path, folder_id)
                    except Exception as pdf_err:
                        logger.error(f"Falha ao gerar/subir Ficha Cadastral (PDF): {pdf_err}")

                # --- PORTÃO DE QUALIDADE (DATA QUALITY GATE) ---
                deal_dict_for_check = {
                    "commodity": commodity,
                    "volume": volume,
                    "price": price,
                    "incoterm": incoterm,
                    "destination": destination,
                    "direcao": direcao
                }

                missing_fields = QualityController.check_completeness(deal_dict_for_check)

                quality_flag = ""
                stage = "Lead Capturado"
                if missing_fields:
                    faltam_str = ", ".join(missing_fields)
                    ts_warn = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    quality_flag = (
                        f"\n\n[EXTRACTOR_WARN | {ts_warn}]\n"
                        f"Campos faltantes detectados: {faltam_str}\n"
                        f"Remetente: {msg.sender} | Grupo: {msg.group_name}\n"
                        f"Acao: Notificacao interna enviada ao assignee."
                    )
                    stage = "Qualificação"
                    logger.warning(
                        "EXTRACTOR_WARN deal_name=%s sender=%s group=%s missing=%s",
                        deal_name, msg.sender, msg.group_name, faltam_str,
                    )

                if direcao == "UNKNOWN":
                    ts_warn = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    quality_flag += (
                        f"\n[EXTRACTOR_WARN | {ts_warn}]\n"
                        f"Direcao nao reconhecida (UNKNOWN) — grupo '{msg.group_name}' "
                        f"nao esta mapeado em GRUPOS_FORNECEDORES/COMPRADORES."
                    )
                    logger.warning(
                        "EXTRACTOR_WARN direcao=UNKNOWN sender=%s group=%s",
                        msg.sender, msg.group_name,
                    )

                try:
                    risk_analysis = analyze_deal_risk(ai_data)
                    score = risk_analysis.get("score", 50)
                except:
                    score = 50

                original_text = msg.content.replace('\n', ' ')[:500]
                drive_header = f"📁 WORKSPACE: {folder_link}\n\n" if folder_link else ""
                notes_str = f"{drive_header}[WHATSAPP] Texto Original: {original_text}{quality_flag}"

                # 1. Cria o Deal no Banco de Dados SQLite
                novo_deal = Deal(
                    name=deal_name,
                    commodity=commodity,
                    direcao=direcao,
                    stage=stage,
                    status="ativo",
                    price=price,
                    volume=volume,
                    volume_unit=unit,
                    currency=currency,
                    incoterm=incoterm,
                    origin=None,
                    destination=destination,
                    source_group=msg.group_name,
                    source_sender=msg.sender,
                    source_message_id=msg.id,
                    assignee=assignee,
                    risk_score=score,
                    notes=notes_str,
                    created_at=msg.timestamp or datetime.utcnow()
                )
                self.session.add(novo_deal)

                # 2. Histórico de Preços
                hist = QuoteHistory(
                    date=msg.timestamp or datetime.utcnow(),
                    commodity=commodity,
                    source="whatsapp",
                    price=price,
                    currency=currency,
                    volume=volume,
                    incoterm=incoterm,
                    location=destination,
                    sender=msg.sender,
                    group_name=msg.group_name
                )
                self.session.add(hist)

                # 3. Espelhamento no Google Sheets (guarda-dupla: DB + coluna A da aba)
                # A flag synced_to_sheets=0 indica que este deal AINDA não foi para a planilha.
                # É setada para 1 apenas após append bem-sucedido — atomicidade no SQLite.
                deal_dict_sync = {
                    "name": deal_name,
                    "created_at": msg.timestamp or datetime.utcnow(),
                    "direcao": direcao,
                    "source_group": msg.group_name,
                    "source_sender": msg.sender,
                    "stage": stage,
                    "commodity": commodity,
                    "destination": destination,
                    "origin": None,
                    "volume": volume,
                    "volume_unit": unit,
                    "incoterm": incoterm,
                    "price": price,
                    "currency": currency,
                    "original_text": notes_str
                }

                try:
                    synced = self.sheets_sync.append_deal_to_sheet(deal_dict_sync)
                    if synced:
                        novo_deal.synced_to_sheets = 1
                except Exception as e:
                    logger.error(f"  ❌ Falha no Sync do Sheets para {deal_name}. Deal salvo apenas no banco. Erro: {e}")

                deals_created += 1
                logger.info(f"  ✓ Pipeline Atualizado: {deal_name} | {direcao} | {assignee} | Risco: {score}")

            # Commita as transações DESSA mensagem específica.
            self.session.commit()

            # Após commit temos IDs reais — agenda follow-ups de campos faltantes.
            for idx2, ai_data in enumerate(extracted_deals):
                if not isinstance(ai_data, dict):
                    continue
                if not ai_data.get("has_quote") or ai_data.get("confidence", 0.0) <= 0.6:
                    continue
                # Recupera o deal recém-criado pelo nome para obter o ID.
                commodity_chk = (ai_data.get("commodity") or msg.commodity or "Indefinida").title()
                deal_base_id = ContextHeuristics.generate_deal_id_base(
                    msg.sender, commodity_chk, msg.timestamp, idx2
                )
                vol_int2 = int(ai_data.get("volume") or msg.volume or 0)
                deal_name_chk = f"{deal_base_id} {vol_int2}"

                from models.database import Deal as DealModel
                saved_deal = self.session.query(DealModel).filter(
                    DealModel.name == deal_name_chk
                ).first()

                if saved_deal and saved_deal.stage == "Qualificação" and msg.sender:
                    chk_dict = {
                        "commodity": commodity_chk,
                        "volume": ai_data.get("volume") or msg.volume,
                        "price": ai_data.get("price") or msg.price,
                        "incoterm": ai_data.get("incoterm"),
                        "destination": ai_data.get("location"),
                        "direcao": ContextHeuristics.infer_direction(
                            ai_data.get("direction"), msg.group_name, msg.sender
                        ),
                    }
                    missing_chk = QualityController.check_completeness(chk_dict)
                    if missing_chk:
                        self._schedule_missing_info_followup(
                            deal_id=saved_deal.id,
                            deal_name=saved_deal.name,
                            assignee=saved_deal.assignee or "Leonardo",
                            sender=msg.sender,
                            group_name=msg.group_name or "",
                            commodity=commodity_chk,
                            missing=missing_chk,
                        )

        except Exception as e_msg:
            self.session.rollback()
            logger.error(f"❌ Falha crítica ao processar a mensagem {msg.id}: {e_msg}. Rolled back apenas esta mensagem.")

        return deals_created

    # ============================================================================
    # FOLLOW-UP DE INFORMAÇÕES FALTANTES
    # ============================================================================

    @staticmethod
    def _build_missing_info_message(commodity: str, missing: list[str]) -> str:
        """
        Gera mensagem WhatsApp perguntando pelos campos em falta.
        Tom: direto, profissional e amigável — sem jargões técnicos.
        """
        produto = commodity if commodity and commodity.lower() not in ("indefinida", "indefinido") else "produto"
        linhas = [
            f"Olá! 👋 Recebemos sua mensagem sobre *{produto}* e já registramos a operação.",
            "",
            "Para avançarmos precisamos confirmar alguns dados:",
        ]
        _labels = {
            "Produto/Especificação": "Qual o produto / especificação exata? (ex: Soja Grão, Açúcar IC45, Frango PDQ)",
            "Volume (MT/Sacas)": "Qual o volume total da operação? (em MT ou sacas)",
            "Incoterm (FOB/CIF/etc)": "Qual o Incoterm desejado? (FOB, CIF, CFR, EXW...)",
            "Target Price": "Qual o preço-alvo? (USD/MT ou USD/Ton)",
            "Porto de Destino (Necessário para CIF)": "Qual o porto de destino?",
        }
        for i, field in enumerate(missing, 1):
            label = _labels.get(field, field)
            linhas.append(f"  {i}. {label}")
        linhas += [
            "",
            "Assim que tivermos essas informações conseguimos dar sequência rapidamente! 🚀",
        ]
        return "\n".join(linhas)

    def _schedule_missing_info_followup(
        self,
        deal_id: int,
        deal_name: str,
        assignee: str,
        sender: str,
        group_name: str,
        commodity: str,
        missing: list[str],
    ) -> None:
        """
        Notifica o time INTERNO da Samba (email + WhatsApp corporativo) que
        um deal entrou em Qualificação por dados insuficientes.

        O humano responsável recebe os detalhes e vai atrás das informações
        com o parceiro externo. Nenhuma mensagem é enviada diretamente ao
        contato externo por este método.

        Também registra um FollowUp no banco como rastreador de pendência.
        """
        # 1. Notificação interna (email + WhatsApp interno)
        try:
            from services.internal_notify import get_notifier
            # Extrai texto original das notes do deal para incluir no email
            from models.database import Deal as _Deal
            _deal_obj = self.session.query(_Deal).filter(_Deal.id == deal_id).first()
            _orig = ""
            if _deal_obj and _deal_obj.notes:
                _n = _deal_obj.notes
                if "[WHATSAPP] Texto Original:" in _n:
                    _orig = _n.split("[WHATSAPP] Texto Original:")[-1].split("[")[0].strip()[:400]
            result = get_notifier().alert_missing_fields(
                deal_id=deal_id,
                deal_name=deal_name,
                commodity=commodity,
                assignee=assignee,
                source_sender=sender,
                source_group=group_name,
                missing=missing,
                original_text=_orig,
            )
            logger.info(
                "  📨 Notificação interna enviada para '%s' — email=%s wpp=%s — faltam: %s",
                assignee, result.get("email"), result.get("whatsapp"), ", ".join(missing),
            )
        except Exception as exc:
            logger.warning("  ⚠️ Falha ao enviar notificação interna: %s", exc)

        # 2. FollowUp como rastreador de pendência no banco
        try:
            from models.database import FollowUp
            msg = self._build_missing_info_message(commodity, missing)
            fu = FollowUp(
                deal_id=deal_id,
                target_person=assignee,        # responsável interno, não o contato externo
                target_group=group_name,
                message=msg,
                due_at=datetime.utcnow() + timedelta(minutes=30),
                status="pendente",
            )
            self.session.add(fu)
            self.session.commit()
        except Exception as exc:
            logger.warning("  ⚠️ Falha ao registrar FollowUp rastreador: %s", exc)

    def audit_and_heal_workspace(self):
        """
        Self-Healing (Auto-Cura) de Nível Enterprise.
        Audita o banco de dados em busca de negócios que não possuem Workspace (Drive) 
        provisionado e executa a criação retroativa, garantindo consistência da infraestrutura.
        """
        if not self.workspace_enabled:
            logger.warning("Auto-Cura abortada: Serviços de Workspace inativos.")
            return

        logger.info("Iniciando Auditoria de Auto-Cura de Workspace...")
        
        # Busca deals ativos onde o campo notes NÃO contém a string de workspace
        deals = self.session.query(Deal).filter(Deal.status == "ativo").all()
        healed_count = 0

        for deal in deals:
            if deal.notes and "📁 WORKSPACE:" in deal.notes:
                continue # Já possui workspace, segue o jogo.

            logger.info(f"  🩹 Falha de infraestrutura detectada no Deal [{deal.name}]. Iniciando auto-cura...")
            
            try:
                # 1. Cria Pasta no Drive
                folder_id, folder_link = self.drive_service.criar_pasta_negocio(deal.name)
                
                # 2. Prepara Ficha Cadastral PDF
                deal_data_full = {
                    "name": deal.name,
                    "created_at": deal.created_at or datetime.utcnow(),
                    "commodity": deal.commodity,
                    "price": deal.price,
                    "currency": deal.currency,
                    "volume": deal.volume,
                    "volume_unit": deal.volume_unit,
                    "incoterm": deal.incoterm or "TBI",
                    "destination": deal.destination or "TBI",
                    "source_group": deal.source_group,
                    "buyer": "TBI", # Fallback para reprocessamento
                    "spec": "TBI",
                    "contract_type": "EXTENSIVO / SPOT",
                    "commission": "TBI"
                }
                
                # Extrai um ID base seguro do nome existente
                safe_id = deal.name.split(" ")[0] if " " in deal.name else deal.name
                pdf_filename = f"FICHA_CADASTRO_{safe_id}_HEALED.pdf"
                temp_dir = os.path.join(ROOT, "temp")
                os.makedirs(temp_dir, exist_ok=True)
                pdf_path = os.path.join(temp_dir, pdf_filename)
                
                # 3. Gera e Sobe PDF
                self.pdf_service.gerar_ficha_pedido(deal_data_full, pdf_path)
                if folder_id:
                    self.drive_service.upload_arquivo(pdf_path, folder_id)

                # 4. Atualiza Banco de Dados
                original_notes = deal.notes or ""
                drive_header = f"📁 WORKSPACE: {folder_link}\n\n"
                
                # Se já tinha tag de Whatsapp, insere o link antes
                if "[WHATSAPP]" in original_notes:
                    deal.notes = original_notes.replace("[WHATSAPP]", f"{drive_header}[WHATSAPP]")
                else:
                    deal.notes = f"{drive_header}{original_notes}"

                self.session.commit()
                
                # 5. [TODO] Sincronizar link retroativo com o Sheets requer um update por ID na API.
                # Por hora, logamos que a cura no banco e drive foi feita.
                logger.info(f"    ✓ Auto-Cura concluída. Workspace provisionado: {folder_link}")
                healed_count += 1

            except Exception as e:
                self.session.rollback()
                logger.error(f"    ✗ Falha ao tentar curar o deal {deal.name}: {e}")

        logger.info(f"Auditoria finalizada. {healed_count} workspaces provisionados retroativamente.")
        self.session.close()

    def enrich_existing_deals(self, limit: int = 20, only_default_score: bool = True):
        """Auditoria de risco em deals existentes."""
        logger.info("Iniciando Auditoria de Risco em Deals existentes (Enriquecimento LLM)...")

        query = self.session.query(Deal).filter(Deal.status == "ativo")
        if only_default_score:
            query = query.filter(Deal.risk_score == 50)
        deals = query.order_by(Deal.created_at.asc()).limit(limit).all()

        if not deals:
            logger.info("Pipeline auditado. Nenhum deal aguardando análise de risco.")
            self.session.close()
            return

        logger.info(f"Analisando perfil de risco de {len(deals)} deals...")
        updated = 0

        for deal in deals:
            deal_data = {
                "commodity":   deal.commodity,
                "volume":      f"{deal.volume} {deal.volume_unit}" if deal.volume else "indefinido",
                "price":       deal.price,
                "currency":    deal.currency,
                "incoterm":    deal.incoterm,
                "origin":      deal.origin,
                "stage":       deal.stage,
                "source_group": deal.source_group,
                "counterparty": deal.source_sender,
            }

            logger.info(f"  → Auditando: {deal.name}")
            try:
                risk = analyze_deal_risk(deal_data)
                deal.risk_score = risk.get("score", 50)
                
                risk_note = (
                    f"\n[RISCO IA - AUDITORIA]\n"
                    f"Score: {deal.risk_score}/100 | Nível: {risk.get('level')}\n"
                    f"Fatores de Atenção: {'; '.join(risk.get('factors', []))}\n"
                    f"Ação Recomendada: {risk.get('recommendation', '')}\n"
                )
                
                deal.notes = (deal.notes or "") + risk_note
                
                self.session.commit()
                updated += 1
                logger.info(f"    ✓ Score consolidado: {deal.risk_score}/100 ({risk.get('level')})")
            except Exception as exc:
                logger.error(f"    ✗ Falha na auditoria de '{deal.name}': {exc}")
                self.session.rollback()

        self.session.close()
        logger.info(f"Auditoria de Risco concluída. {updated}/{len(deals)} deals re-avaliados.")

if __name__ == "__main__":
    agent = ExtractorAgent()
    # Executa a Auto-Cura antes de processar novas mensagens
    agent.audit_and_heal_workspace()
    agent.process_pending_messages()