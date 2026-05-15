"""
services/market_data.py
=======================
Motor de Ingestao de Dados de Mercado (Fisico + Bolsas + Macro).
Inteligencia senior de scrapers blindados e fallbacks.
"""
import sys
import time
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# Limites matematicos aceitaveis (BRL por TONELADA) para evitar lixo HTML
SANITY_BOUNDS_BRL_TON = {
    "SOY":   (1000.0, 4500.0),
    "CORN":  (500.0,  2500.0),
    "SUGAR": (1500.0, 5000.0),
}

class PhysicalMarketScraper:

    @staticmethod
    def _parse_price(s: str) -> float:
        clean = re.sub(r'[^0-9,\.]', '', s.strip())
        if not clean:
            return 0.0
        # Formato brasileiro: 120,50 ou 1.200,50
        if ',' in clean and '.' in clean:
            clean = clean.replace('.', '').replace(',', '.')
        elif ',' in clean:
            clean = clean.replace(',', '.')
        try:
            return float(clean)
        except Exception:
            return 0.0

    @staticmethod
    def _parse_location(s: str):
        s = s.strip()
        m = re.search(r'([A-Za-zÀ-ÖØ-öø-ÿ\s\-\.]+)[/\-]\s*([A-Z]{2})\b', s)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return None, None

    @classmethod
    def _save_if_new(cls, db, produto: str, cidade: str, uf: str,
                     preco_ton: float, fonte: str, limite_tempo) -> bool:
        bounds = SANITY_BOUNDS_BRL_TON.get(produto, (10.0, 100000.0))
        if not (bounds[0] <= preco_ton <= bounds[1]):
            return False
        existe = db.query(PrecoFisicoRaw).filter(
            PrecoFisicoRaw.produto == produto,
            PrecoFisicoRaw.uf == uf,
            PrecoFisicoRaw.cidade == cidade,
            PrecoFisicoRaw.timestamp >= limite_tempo,
        ).first()
        if not existe:
            db.add(PrecoFisicoRaw(
                produto=produto, uf=uf, cidade=cidade,
                preco_brl_ton=round(preco_ton, 2),
                fonte=fonte, timestamp=datetime.utcnow(),
            ))
            return True
        return False

    # ── FONTE 1: Agrolink ────────────────────────────────────────────
    _AGROLINK_URLS = {
        "SOY":   "https://www.agrolink.com.br/cotacoes/graos?cultura=soja",
        "CORN":  "https://www.agrolink.com.br/cotacoes/graos?cultura=milho",
        "SUGAR": "https://www.agrolink.com.br/cotacoes/outros?cultura=acucar",
    }
    _AGROLINK_KG = {"SOY": 60, "CORN": 60, "SUGAR": 50}

    @classmethod
    def _scrape_agrolink(cls, db, produto: str, limite_tempo) -> int:
        url = cls._AGROLINK_URLS.get(produto)
        if not url:
            return 0
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return 0
        soup = BeautifulSoup(resp.content, "html.parser")
        kg = cls._AGROLINK_KG[produto]
        count = 0
        for row in soup.find_all("tr"):
            cols = row.find_all(["td", "th"])
            if len(cols) < 3:
                continue
            loc_text = cols[0].get_text(strip=True)
            preco_text = cols[2].get_text(strip=True)   # coluna "Preço"
            cidade, uf = cls._parse_location(loc_text)
            preco_saca = cls._parse_price(preco_text)
            if not (cidade and uf and preco_saca > 0):
                continue
            preco_ton = preco_saca * 1000 / kg
            if cls._save_if_new(db, produto, cidade, uf, preco_ton, "Agrolink", limite_tempo):
                count += 1
        return count

    # ── FONTE 2: CEPEA/ESALQ ─────────────────────────────────────────
    _CEPEA_URLS = {
        "SOY":   ("https://cepea.esalq.usp.br/br/indicador/soja.aspx",   "Paranaguá", "PR", 60),
        "CORN":  ("https://cepea.esalq.usp.br/br/indicador/milho.aspx",  "Campinas",  "SP", 60),
        "SUGAR": ("https://cepea.esalq.usp.br/br/indicador/acucar.aspx", "São Paulo", "SP", 50),
    }

    @classmethod
    def _scrape_cepea(cls, db, produto: str, limite_tempo) -> int:
        cfg = cls._CEPEA_URLS.get(produto)
        if not cfg:
            return 0
        url, cidade, uf, kg = cfg
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return 0
        soup = BeautifulSoup(resp.content, "html.parser")
        # CEPEA tem tabela com id "imagenet-indicador1" ou classe "tableBig"
        for tbl in soup.find_all("table"):
            for row in tbl.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                for col in cols[1:3]:   # colunas "À vista" e "À prazo"
                    txt = col.get_text(strip=True)
                    preco_saca = cls._parse_price(txt)
                    if preco_saca > 0:
                        preco_ton = preco_saca * 1000 / kg
                        if cls._save_if_new(db, produto, cidade, uf, preco_ton, "CEPEA", limite_tempo):
                            return 1
        return 0

    # ── FONTE 3: NoticiasAgricolas ───────────────────────────────────
    _NOTICIAS_URLS = {
        "SOY":   "https://www.noticiasagricolas.com.br/cotacoes/soja/soja-mercado-fisico-sindicatos-e-cooperativas",
        "CORN":  "https://www.noticiasagricolas.com.br/cotacoes/milho/milho-mercado-fisico-sindicatos-e-cooperativas",
        "SUGAR": "https://www.noticiasagricolas.com.br/cotacoes/acucar/acucar-cristal-mercado-fisico",
    }

    @classmethod
    def _scrape_noticias(cls, db, produto: str, limite_tempo) -> int:
        url = cls._NOTICIAS_URLS.get(produto)
        if not url:
            return 0
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return 0
        soup = BeautifulSoup(resp.content, "html.parser")
        kg = 50 if produto == "SUGAR" else 60
        count = 0
        for row in soup.find_all("tr"):
            cols = row.find_all(["td", "th"])
            if len(cols) < 2:
                continue
            cidade, uf = cls._parse_location(cols[0].get_text(strip=True))
            preco_saca = cls._parse_price(cols[1].get_text(strip=True))
            if not (cidade and uf and preco_saca > 0):
                continue
            preco_ton = preco_saca * 1000 / kg
            if cls._save_if_new(db, produto, cidade, uf, preco_ton, "NoticiasAgricolas", limite_tempo):
                count += 1
        return count

    # ── ORQUESTRADOR ─────────────────────────────────────────────────
    @classmethod
    def scrape_all_markets(cls, db):
        logger.info("[SCRAPER] Iniciando varredura com cadeia de fallback...")
        limite_tempo = datetime.utcnow() - timedelta(hours=6)
        total = 0
        for produto in ["SOY", "CORN", "SUGAR"]:
            for nome, metodo in [
                ("Agrolink",          cls._scrape_agrolink),
                ("CEPEA",             cls._scrape_cepea),
                ("NoticiasAgricolas", cls._scrape_noticias),
            ]:
                try:
                    n = metodo(db, produto, limite_tempo)
                    logger.info(f"[SCRAPER] {nome}/{produto}: {n} registros")
                    total += n
                    if n > 0:
                        break   # fonte funcionou, próximo produto
                except Exception as e:
                    logger.warning(f"[SCRAPER] {nome}/{produto} falhou: {e}")
        db.commit()
        logger.info(f"[SCRAPER] Total: {total} registros inseridos.")

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
            closes = cls._fetch_batch()
            vals = cls._close(closes, "BZ=F")
            if vals:
                return cls._safe_float((vals[-1] * 7.3) + 50, cls.FALLBACK_BUNKER)
        except Exception: pass
        last = db.query(MarketSnapshot).filter(MarketSnapshot.bunker_vlsfo > 0).order_by(MarketSnapshot.timestamp.desc()).first()
        return last.bunker_vlsfo if last else cls.FALLBACK_BUNKER

    @classmethod
    def get_daily_hire_panamax(cls, db) -> float:
        try:
            closes = cls._fetch_batch()
            vals = cls._close(closes, "^BDI")
            if vals:
                return cls._safe_float(vals[-1] * 12, cls.FALLBACK_DAILY_HIRE)
        except Exception: pass
        last = db.query(MarketSnapshot).filter(MarketSnapshot.daily_hire > 0).order_by(MarketSnapshot.timestamp.desc()).first()
        return last.daily_hire if last else cls.FALLBACK_DAILY_HIRE

    @classmethod
    def get_usd_brl(cls) -> float:
        try:
            closes = cls._fetch_batch()
            vals = cls._close(closes, "USDBRL=X")
            if vals:
                return cls._safe_float(vals[-1], cls.FALLBACK_USD_BRL)
        except Exception: pass
        return cls.FALLBACK_USD_BRL

    # Cache interno para o batch download — evita chamadas duplicadas no mesmo ciclo
    _yf_batch_cache: dict = {}
    _yf_batch_ts: float = 0.0
    _YF_BATCH_TTL: float = 240.0  # 4 min

    @classmethod
    def _fetch_batch(cls) -> "pd.DataFrame":
        """
        Baixa TODOS os tickers de uma vez com yf.download() (threaded).
        Cache interno de 4 min evita re-download dentro do mesmo ciclo de update.
        Retorna DataFrame MultiIndex [Close][sym] ou DataFrame simples (1 ticker).
        """
        now = time.monotonic()
        if now - cls._yf_batch_ts < cls._YF_BATCH_TTL and cls._yf_batch_cache:
            return cls._yf_batch_cache.get("df", pd.DataFrame())

        symbols = ["ZS=F", "ZC=F", "SB=F", "USDBRL=X",
                   "ZM=F", "CC=F", "KC=F", "CT=F",
                   "BZ=F", "^BDI"]
        try:
            df = yf.download(
                symbols, period="2d",
                auto_adjust=True, progress=False, threads=True,
                timeout=4,
            )
            # yf.download com >1 símbolo → MultiIndex (Close, sym); com 1 → plano
            if isinstance(df.columns, pd.MultiIndex):
                closes = df["Close"] if "Close" in df.columns.get_level_values(0) else pd.DataFrame()
            else:
                closes = df[["Close"]] if "Close" in df.columns else pd.DataFrame()
            cls._yf_batch_cache = {"df": closes}
            cls._yf_batch_ts = now
            return closes
        except Exception as e:
            logger.warning("[BATCH] yf.download falhou: %s", e)
            return pd.DataFrame()

    @classmethod
    def _close(cls, closes: "pd.DataFrame", sym: str) -> list:
        """Retorna lista de closes para um símbolo; vazia se ausente."""
        try:
            if sym in closes.columns:
                return closes[sym].dropna().tolist()
            if "Close" in closes.columns:   # fallback: df plano (1 símbolo)
                return closes["Close"].dropna().tolist()
        except Exception:
            pass
        return []

    @classmethod
    def get_bolsas_usd_mt(cls):
        res = {"cbot_soy_usd_mt": cls.FALLBACK_SOY, "cbot_corn_usd_mt": cls.FALLBACK_CORN, "ice_sugar_usd_mt": cls.FALLBACK_SUGAR}
        try:
            closes = cls._fetch_batch()
            soy_vals   = cls._close(closes, "ZS=F")
            corn_vals  = cls._close(closes, "ZC=F")
            sugar_vals = cls._close(closes, "SB=F")

            if soy_vals:
                soy_raw = soy_vals[-1]
                res["cbot_soy_usd_mt"] = (soy_raw / 100 if soy_raw > 100 else soy_raw) * 36.7437
            if corn_vals:
                corn_raw = corn_vals[-1]
                res["cbot_corn_usd_mt"] = (corn_raw / 100 if corn_raw > 100 else corn_raw) * 39.3680
            if sugar_vals:
                res["ice_sugar_usd_mt"] = sugar_vals[-1] * 22.0462
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
        Busca cotações ao vivo dos 4 tickers estendidos via batch download.
        Uma única chamada yf.download() substituiu 4 chamadas sequenciais.
        """
        result: dict = {}
        closes = cls._fetch_batch()
        for sym, cfg in cls.EXTENDED_TICKERS.items():
            try:
                vals = cls._close(closes, sym)
                if not vals:
                    raise ValueError("sem dados")
                current_raw = float(vals[-1])
                prev_raw    = float(vals[-2]) if len(vals) >= 2 else current_raw
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
            from sqlalchemy import text as _text
            sql = _text("""
                SELECT DISTINCT ON (cidade, uf)
                    cidade || '/' || uf AS "PRACA FISICA",
                    CASE
                        WHEN produto = 'SUGAR' THEN ROUND((preco_brl_ton * 50 / 1000)::numeric, 2)
                        ELSE ROUND((preco_brl_ton * 60 / 1000)::numeric, 2)
                    END AS "PRECO (SACA)",
                    ROUND(preco_brl_ton::numeric, 2) AS "PRECO (TON)",
                    fonte AS "FONTE",
                    LEFT(timestamp::text, 10) AS "DATA"
                FROM tb_preco_fisico_raw
                WHERE UPPER(produto) = UPPER(:prod)
                ORDER BY cidade, uf, timestamp DESC
                LIMIT 20
            """)
            with engine.connect() as conn:
                df = pd.read_sql(sql.bindparams(prod=produto), conn)
            return df
        except Exception as e:
            logger.error(f"[FACADE] Erro ao buscar pracas: {e}")
            return pd.DataFrame()

market_data = MarketDataFacade()

if __name__ == "__main__":
    update_all_market_data()