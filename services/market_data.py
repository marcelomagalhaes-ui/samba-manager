"""
services/market_data.py
=======================
Motor de Ingestao de Dados de Mercado (Fisico + Bolsas + Macro).
Inteligencia senior de scrapers blindados e fallbacks.
"""
import sys
import logging
import requests
import re
import yfinance as yf
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import get_session, get_engine, PrecoFisicoRaw, MarketSnapshot, BolsasBase

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ==========================================================
# CONFIGURACOES DO SCRAPER (MERCADO FISICO)
# ==========================================================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

SOURCES = {
    "SOY": [
        {"url": "https://www.noticiasagricolas.com.br/cotacoes/soja/soja-mercado-fisico-sindicatos-e-cooperativas", "fonte": "Noticias_Agricolas"}
    ],
    "CORN": [
        {"url": "https://www.noticiasagricolas.com.br/cotacoes/milho/milho-mercado-fisico-sindicatos-e-cooperativas", "fonte": "Noticias_Agricolas"}
    ],
    "SUGAR": [
        {"url": "https://www.noticiasagricolas.com.br/cotacoes/acucar/acucar-cristal-mercado-fisico", "fonte": "Noticias_Agricolas"}
    ]
}

# Limites matematicos aceitaveis (BRL por TONELADA) para evitar lixo HTML
SANITY_BOUNDS_BRL_TON = {
    "SOY":   (1000.0, 4500.0),
    "CORN":  (500.0,  2500.0),
    "SUGAR": (1500.0, 5000.0)
}

class PhysicalMarketScraper:
    @staticmethod
    def _parse_price(price_str: str) -> float:
        clean_str = re.sub(r'[^0-9,]', '', price_str)
        if not clean_str: return 0.0
        return float(clean_str.replace(',', '.'))

    @staticmethod
    def _parse_location(loc_str: str):
        match = re.search(r'([A-Za-zÀ-ÖØ-öø-ÿ\s\-]+)/([A-Z]{2})', loc_str.strip())
        if match: return match.group(1).strip(), match.group(2).strip()
        match_fallback = re.search(r'([A-Za-zÀ-ÖØ-öø-ÿ\s\-]+)[/\-\s]+([A-Z]{2})', loc_str.strip())
        if match_fallback: return match_fallback.group(1).strip(), match_fallback.group(2).strip()
        return None, None

    @classmethod
    def scrape_all_markets(cls, db):
        logger.info("[SCRAPER] Iniciando varredura de Mercados Fisicos...")
        registros_inseridos = 0
        limite_tempo = datetime.utcnow() - timedelta(hours=6)

        for produto, configs in SOURCES.items():
            for config in configs:
                try:
                    resp = requests.get(config["url"], headers=HEADERS, timeout=15)
                    if resp.status_code != 200: continue
                    soup = BeautifulSoup(resp.content, "html.parser")
                    bounds = SANITY_BOUNDS_BRL_TON.get(produto, (10.0, 100000.0))

                    for row in soup.find_all("tr"):
                        cols = row.find_all(["td", "th"])
                        if len(cols) < 2: continue
                        
                        cidade, uf = cls._parse_location(cols[0].get_text(strip=True))
                        preco_saca = cls._parse_price(cols[1].get_text(strip=True))

                        if cidade and uf and preco_saca > 0:
                            # 50kg para acucar, 60kg para graos
                            mult = (1000 / 50) if produto == "SUGAR" else (1000 / 60)
                            preco_ton = preco_saca * mult

                            # Sanity Check para evitar salvar variacoes percentuais ou comissoes
                            if bounds[0] <= preco_ton <= bounds[1]:
                                ja_existe = db.query(PrecoFisicoRaw).filter(
                                    PrecoFisicoRaw.produto == produto,
                                    PrecoFisicoRaw.uf == uf,
                                    PrecoFisicoRaw.cidade == cidade,
                                    PrecoFisicoRaw.timestamp >= limite_tempo
                                ).first()

                                if not ja_existe:
                                    db.add(PrecoFisicoRaw(
                                        produto=produto, uf=uf, cidade=cidade,
                                        preco_brl_ton=round(preco_ton, 2),
                                        fonte=config["fonte"], timestamp=datetime.utcnow()
                                    ))
                                    registros_inseridos += 1
                except Exception as e:
                    logger.error(f"[SCRAPER] Erro ao extrair {produto}: {e}")

        db.commit()
        logger.info(f"[SCRAPER] Concluido. {registros_inseridos} registros processados.")

# ==========================================================
# EXTERNAL APIs & FALLBACKS
# ==========================================================
class ExternalDataService:
    FALLBACK_DIESEL = 6.10
    FALLBACK_BUNKER = 650.0
    FALLBACK_USD_BRL = 5.50
    FALLBACK_DAILY_HIRE = 18000.0
    FALLBACK_SOY = 450.0
    FALLBACK_CORN = 200.0
    FALLBACK_SUGAR = 500.0

    @staticmethod
    def _safe_float(value, fallback):
        try:
            val = float(value)
            return fallback if val <= 0 else val
        except: return fallback

    @classmethod
    def get_diesel_price(cls, db) -> float:
        try:
            resp = requests.get("https://combustivelapi.com.br/api/precos/sp", timeout=10)
            if resp.status_code == 200:
                val = resp.json().get("diesel_s10")
                if val: return cls._safe_float(val, cls.FALLBACK_DIESEL)
        except Exception: pass
        
        last = db.query(MarketSnapshot).filter(MarketSnapshot.diesel_s10 > 0).order_by(MarketSnapshot.timestamp.desc()).first()
        return last.diesel_s10 if last else cls.FALLBACK_DIESEL

    @classmethod
    def get_bunker_proxy_price(cls, db) -> float:
        try:
            hist = yf.Ticker("BZ=F").history(period="1d")
            if not hist.empty:
                return cls._safe_float((hist["Close"].iloc[-1] * 7.3) + 50, cls.FALLBACK_BUNKER)
        except Exception: pass
        last = db.query(MarketSnapshot).filter(MarketSnapshot.bunker_vlsfo > 0).order_by(MarketSnapshot.timestamp.desc()).first()
        return last.bunker_vlsfo if last else cls.FALLBACK_BUNKER

    @classmethod
    def get_daily_hire_panamax(cls, db) -> float:
        try:
            hist = yf.Ticker("^BDI").history(period="1d")
            if not hist.empty:
                return cls._safe_float(hist["Close"].iloc[-1] * 12, cls.FALLBACK_DAILY_HIRE)
        except Exception: pass
        last = db.query(MarketSnapshot).filter(MarketSnapshot.daily_hire > 0).order_by(MarketSnapshot.timestamp.desc()).first()
        return last.daily_hire if last else cls.FALLBACK_DAILY_HIRE

    @classmethod
    def get_usd_brl(cls) -> float:
        try:
            hist = yf.Ticker("USDBRL=X").history(period="1d")
            if not hist.empty: return cls._safe_float(hist["Close"].iloc[-1], cls.FALLBACK_USD_BRL)
        except Exception: pass
        return cls.FALLBACK_USD_BRL

    @classmethod
    def get_bolsas_usd_mt(cls):
        res = {"cbot_soy_usd_mt": cls.FALLBACK_SOY, "cbot_corn_usd_mt": cls.FALLBACK_CORN, "ice_sugar_usd_mt": cls.FALLBACK_SUGAR}
        try:
            soy_raw = yf.Ticker("ZS=F").history(period="1d")["Close"].iloc[-1]
            corn_raw = yf.Ticker("ZC=F").history(period="1d")["Close"].iloc[-1]
            sugar_raw = yf.Ticker("SB=F").history(period="1d")["Close"].iloc[-1]

            res["cbot_soy_usd_mt"] = (soy_raw / 100 if soy_raw > 100 else soy_raw) * 36.7437
            res["cbot_corn_usd_mt"] = (corn_raw / 100 if corn_raw > 100 else corn_raw) * 39.3680
            res["ice_sugar_usd_mt"] = sugar_raw * 22.0462
        except Exception as e:
            logger.warning(f"[FINANCE] Falha na extracao live. Aplicando fallbacks. Erro: {e}")
        return res

    # ------------------------------------------------------------------
    # Tickers estendidos para o Ticker do painel
    # Unidades originais no Yahoo Finance e fator de conversão para USD/MT:
    #
    #   ZM=F  Soybean Meal   → USD / short ton  ×1.10231  → USD/MT
    #   CC=F  Cocoa ICE      → USD / metric ton  ×1.0      → USD/MT (nativo)
    #   KC=F  Coffee C ICE   → cents / pound     ×22.0462  → USD/MT
    #   CT=F  Cotton #2 ICE  → cents / pound     ×22.0462  → USD/MT
    # ------------------------------------------------------------------
    EXTENDED_TICKERS: dict = {
        "ZM=F": {
            "key":   "FARELO SOJA (USD/MT)",
            "conv":  lambda p: p * 1.10231,      # USD/short_ton → USD/MT
            "fb":    320.0,
        },
        "CC=F": {
            "key":   "CACAU ICE (USD/MT)",
            "conv":  lambda p: p,                 # já USD/MT
            "fb":    8000.0,
        },
        "KC=F": {
            "key":   "CAFE ICE (USD/MT)",
            "conv":  lambda p: p * 22.0462,       # cents/lb → USD/MT
            "fb":    5500.0,
        },
        "CT=F": {
            "key":   "ALGODAO ICE (USD/MT)",
            "conv":  lambda p: p * 22.0462,       # cents/lb → USD/MT
            "fb":    1700.0,
        },
    }

    @classmethod
    def get_extended_tickers(cls) -> dict:
        """
        Busca cotações ao vivo dos 4 tickers estendidos via Yahoo Finance
        e devolve dict {key: {valor_usd_mt, variacao_pct}}.

        Fallback por ticker individual — falha isolada não derruba o conjunto.
        """
        result: dict = {}
        for sym, cfg in cls.EXTENDED_TICKERS.items():
            try:
                hist = yf.Ticker(sym).history(period="2d")
                if hist.empty:
                    raise ValueError("sem dados")
                closes = hist["Close"].dropna()
                current_raw = float(closes.iloc[-1])
                prev_raw    = float(closes.iloc[-2]) if len(closes) >= 2 else current_raw

                current_usd = cfg["conv"](current_raw)
                prev_usd    = cfg["conv"](prev_raw)
                variacao    = ((current_usd - prev_usd) / prev_usd * 100) if prev_usd else 0.0

                result[cfg["key"]] = {
                    "valor":    round(current_usd, 2),
                    "variacao": round(variacao, 4),
                }
                logger.debug("[EXTENDED] %s %.2f USD/MT (var %.2f%%)", sym, current_usd, variacao)
            except Exception as e:
                logger.warning("[EXTENDED] Fallback %s: %s", sym, e)
                result[cfg["key"]] = {"valor": cfg["fb"], "variacao": 0.0}
        return result

# ==========================================================
# ORQUESTRADOR PRINCIPAL
# ==========================================================
def update_all_market_data():
    logger.info("[ORCHESTRATOR] Iniciando ciclo de atualizacao global de mercado...")
    session = get_session()
    
    try:
        try:
            PhysicalMarketScraper.scrape_all_markets(session)
        except Exception as e_scrape:
            session.rollback()
            logger.warning(f"[ORCHESTRATOR] Scraper fisico falhou (ignorado): {e_scrape}")

        bolsas = ExternalDataService.get_bolsas_usd_mt()

        snapshot = MarketSnapshot(
            timestamp=datetime.utcnow(),
            usd_brl=float(ExternalDataService.get_usd_brl()),
            cbot_soy_usd_mt=float(round(bolsas["cbot_soy_usd_mt"], 2)),
            cbot_corn_usd_mt=float(round(bolsas["cbot_corn_usd_mt"], 2)),
            ice_sugar_usd_mt=float(round(bolsas["ice_sugar_usd_mt"], 2)),
            diesel_s10=float(round(ExternalDataService.get_diesel_price(session), 2)),
            bunker_vlsfo=float(round(ExternalDataService.get_bunker_proxy_price(session), 2)),
            daily_hire=float(round(ExternalDataService.get_daily_hire_panamax(session), 2)),
        )
        session.add(snapshot)

        agora = datetime.utcnow()
        for comm, price in [
            ("SOY",   snapshot.cbot_soy_usd_mt),
            ("CORN",  snapshot.cbot_corn_usd_mt),
            ("SUGAR", snapshot.ice_sugar_usd_mt),
        ]:
            session.add(BolsasBase(
                commodity=comm, contract="LIVE",
                price_raw=price, unit_original="USD/MT",
                conversion_factor=1.0, price_usd_mt=price,
                timestamp=agora, source_flag="YAHOO_LIVE",
            ))

        session.commit()
        logger.info("[ORCHESTRATOR] Sincronizacao de snapshots salva com sucesso.")

    except Exception as e:
        session.rollback()
        logger.error(f"[ORCHESTRATOR] Erro critico: {e}")
    finally:
        session.close()

# ==========================================================
# COMUNICACAO COM O FRONTEND
# ==========================================================
class MarketDataFacade:
    def get_market_overview(self):
        session = get_session()
        try:
            snaps = session.query(MarketSnapshot).order_by(MarketSnapshot.timestamp.desc()).limit(2).all()
            if not snaps:
                return {"MERCADO": {"valor": 0.0, "variacao": 0.0}}

            current = snaps[0]
            prev = snaps[1] if len(snaps) > 1 else current

            def calc_var(c, p):
                if p == 0: return 0.0
                return ((c - p) / p) * 100

            return {
                "SOY_CBOT (USD/MT)": {
                    "valor": current.cbot_soy_usd_mt,
                    "variacao": calc_var(current.cbot_soy_usd_mt, prev.cbot_soy_usd_mt)
                },
                "CORN_CBOT (USD/MT)": {
                    "valor": current.cbot_corn_usd_mt,
                    "variacao": calc_var(current.cbot_corn_usd_mt, prev.cbot_corn_usd_mt)
                },
                "SUGAR_ICE (USD/MT)": {
                    "valor": current.ice_sugar_usd_mt,
                    "variacao": calc_var(current.ice_sugar_usd_mt, prev.ice_sugar_usd_mt)
                },
                "USD/BRL": {
                    "valor": current.usd_brl,
                    "variacao": calc_var(current.usd_brl, prev.usd_brl)
                }
            }
        except Exception as e:
            logger.error(f"[FACADE] Erro no Ticker: {e}")
            return {}
        finally:
            session.close()

    def get_extended_overview(self) -> dict:
        """
        Overview completo para o Ticker do painel:
          base (DB)  →  SOY, CORN, SUGAR, USD/BRL
          + live     →  FARELO SOJA, CACAU, CAFE, ALGODAO

        Ordem do ticker:
          Soja → Milho → Açúcar → Farelo → Cacau → Café → Algodão → USD/BRL

        Cada entry: {valor: float USD/MT, variacao: float %}
        """
        base     = self.get_market_overview()          # usa snapshot do DB
        extended = ExternalDataService.get_extended_tickers()  # live Yahoo

        # Monta dict em ordem lógica para o ticker
        ordered: dict = {}
        for key in ("SOY_CBOT (USD/MT)", "CORN_CBOT (USD/MT)", "SUGAR_ICE (USD/MT)"):
            if key in base:
                ordered[key] = base[key]
        ordered.update(extended)                        # farelo, cacau, café, algodão
        if "USD/BRL" in base:
            ordered["USD/BRL"] = base["USD/BRL"]       # câmbio sempre por último

        return ordered

    def get_pracas_fisicas(self, produto: str):
        engine = get_engine()
        try:
            df = pd.read_sql(f"""
                SELECT 
                    cidade || '/' || uf AS "PRACA FISICA",
                    CASE 
                        WHEN produto = 'SUGAR' THEN preco_brl_ton * 50 / 1000
                        ELSE preco_brl_ton * 60 / 1000
                    END AS "PRECO (SACA)",
                    preco_brl_ton AS "PRECO (TON)",
                    fonte AS "FONTE",
                    SUBSTR(timestamp, 1, 10) AS "DATA"
                FROM tb_preco_fisico_raw
                WHERE produto = '{produto}'
                ORDER BY timestamp DESC
                LIMIT 15
            """, engine)
            return df
        except Exception as e:
            logger.error(f"[FACADE] Erro ao buscar pracas: {e}")
            return pd.DataFrame()

market_data = MarketDataFacade()

if __name__ == "__main__":
    update_all_market_data()