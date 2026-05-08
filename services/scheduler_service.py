"""
services/scheduler_service.py
=============================
Motor de Agendamento Autónomo da Samba Export (Trabalhador de Background).
Garante que os dados de mercado (Físico, Bolsas e Macro) sejam atualizados
antes das 8h00 e periodicamente ao longo do dia.
"""
import sys
import time
import logging
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Garantir path absoluto
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.market_data import update_all_market_data

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()

def job_update_market():
    """Missão: Atualizar o Cofre de Dados (Físico + Bolsas + Macro)."""
    logger.info("🤖 [SCHEDULER] Iniciando rotina de atualização de mercado...")
    try:
        update_all_market_data()
        logger.info("✅ [SCHEDULER] Rotina concluída com sucesso.")
    except Exception as e:
        logger.error(f"❌ [SCHEDULER] Falha na rotina de mercado: {e}")

def start_scheduler():
    """Injeta as rotinas no motor e mantém o processo vivo."""
    if not scheduler.running:
        
        # 1. GARANTIA MATINAL (A Regra de Ouro)
        # Corre todos os dias, religiosamente, às 07h30 da manhã.
        scheduler.add_job(
            job_update_market,
            CronTrigger(hour=7, minute=30),
            id='job_morning_guarantee',
            replace_existing=True
        )

        # 2. VARREDURA PERIÓDICA (O Pulso do Mercado)
        # Corre a cada 2 horas para apanhar as flutuações durante o expediente.
        scheduler.add_job(
            job_update_market,
            'interval',
            hours=2,
            id='job_periodic_update',
            replace_existing=True
        )

        scheduler.start()
        logger.info("=========================================================")
        logger.info("⏰ MOTOR DE AGENDAMENTO (APSCHEDULER) ATIVADO")
        logger.info("-> Job 1: Briefing Matinal agendado para as 07h30 diárias.")
        logger.info("-> Job 2: Varredura Periódica agendada para cada 2 horas.")
        logger.info("=========================================================")
        
        # Mantém o script a correr num loop infinito para o BackgroundScheduler não morrer
        try:
            while True:
                time.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            stop_scheduler()

def stop_scheduler():
    """Desliga os robôs de forma limpa."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("🛑 Motor de Agendamento desligado.")

if __name__ == "__main__":
    start_scheduler()