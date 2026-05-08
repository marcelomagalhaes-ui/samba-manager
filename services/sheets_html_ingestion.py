"""
services/sheets_html_ingestion.py
==================================
Ingere exportações HTML das abas do Google Sheets da Samba Export.

Abas de Pipeline (mesmo schema: JOB, STATUS, OFERTAouPEDIDO, PRODUTO...):
  todos andamento.html, Para Vender.html, Declinados.html,
  Andamento Vietnã.html

Abas de Preços:
  valores comuns .html  → deals com preço real por broker
  rokane.html           → price sheet do Rokane

Fornecedores:
  FORNECEDORES.html     → tabela StrategicData

Anti-duplicata: verifica campo name (JOB ID) antes de inserir.

Uso:
    python main.py ingest-sheets
    python services/sheets_html_ingestion.py
"""
from __future__ import annotations

import json
import logging
import re
import sys
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from models.database import get_engine, Deal, StrategicData, create_tables

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

SHEETS_DIR = ROOT / "data" / "sheets"

# ── Mapeamento de STATUS → Stage do pipeline ──────────────────────────────────
STATUS_MAP: dict[str, str] = {
    "pendente comprador":      "Qualificação",
    "pendente vendedor":       "Proposta Enviada",
    "finder":                  "Lead Capturado",
    "parada":                  "Lead Capturado",
    "informacao":              "Lead Capturado",
    "em negociacao":           "Negociação",
    "em negociação":           "Negociação",
    "em due diligence":        "Em Due Diligence",
    "contrato assinado":       "Contrato Assinado",
    "perdido":                 "Declinado",
    "abandonado pela samba":   "Declinado",
    "cancelado":               "Declinado",
}

# ── Mapeamento OFERTAouPEDIDO → direcao (BID/ASK) ────────────────────────────
DIRECAO_MAP: dict[str, str] = {
    "pedido":    "BID",
    "oferta":    "ASK",
    "informacao": "UNKNOWN",
}

# ── Atribuição de sócios por commodity ────────────────────────────────────────
SOCIO_KEYWORDS: dict[str, list[str]] = {
    "Leonardo": ["soja", "milho", "trigo", "arroz", "feijao", "feijão", "sorgo",
                 "farelo", "ddg", "farinha", "graos", "grãos", "alubia", "pulses"],
    "Nivio":    ["açúcar", "acucar", "sugar", "etanol", "ethanol", "algodao",
                 "algodão", "cotton", "biodiesel", "diesel", "combustivel",
                 "oleo", "óleo", "girassol", "palma", "soja oleo"],
    "Marcelo":  ["café", "cafe", "coffee", "cacau", "cocoa", "frango", "chicken",
                 "boi", "beef", "gado", "cordeiro", "prata", "proteina"],
}
SOCIO_DEFAULT = "Leonardo"

# ── Arquivos de pipeline (todos têm o mesmo schema de colunas) ─────────────────
PIPELINE_FILES = [
    "todos andamento.html",
    "Para Vender.html",
    "Declinados.html",
    "Andamento Vietnã.html",
]

# Status que devem ser marcados como "arquivado" em vez de "ativo"
ARCHIVED_STATUS = {"perdido", "abandonado pela samba", "cancelado"}


# ── Utilidades ────────────────────────────────────────────────────────────────

def _read_html(path: Path) -> BeautifulSoup | None:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return BeautifulSoup(path.read_bytes(), "html.parser")
        except Exception:
            pass
    return None


def _normalize_commodity(raw: str) -> str:
    """Capitaliza e limpa o nome da commodity."""
    if not raw:
        return "Indefinida"
    c = raw.strip().lower()
    # Normaliza variações comuns
    mappings = {
        "acucar": "Açúcar",
        "açúcar ic45": "Açúcar IC45",
        "açúcar vhp": "Açúcar VHP",
        "ic45": "Açúcar IC45",
        "vhp": "Açúcar VHP",
        "soja premium": "Soja",
        "soja padrao": "Soja",
        "soja nao gmo exportacao": "Soja",
        "chicken paw": "Frango",
        "chicken paw chicken parts": "Frango",
        "frango": "Frango",
        "milho humano": "Milho",
        "milho animal": "Milho",
        "milho amarelo": "Milho",
        "milho amarelo humano": "Milho",
        "oleo soja": "Óleo de Soja",
        "oleo girassol": "Óleo de Girassol",
        "oleo vegetal": "Óleo Vegetal",
        "etanol": "Etanol",
        "etanol de milho": "Etanol",
        "farelo soja": "Farelo de Soja",
        "gado": "Boi",
        "beef": "Boi",
    }
    for k, v in mappings.items():
        if k in c:
            return v
    # Capitaliza palavra por palavra
    return " ".join(w.capitalize() for w in raw.strip().split())


def _resolve_assignee(commodity: str) -> str:
    c_lower = (commodity or "").lower()
    for socio, keywords in SOCIO_KEYWORDS.items():
        for kw in keywords:
            if kw in c_lower:
                return socio
    return SOCIO_DEFAULT


def _extract_price_from_text(text: str) -> tuple[float | None, str | None]:
    """
    Tenta extrair preço e moeda de texto livre.
    Ex: 'USD 485,00 MT', 'US$ 480.00 per Mt', 'FOB US$ 485'
    Retorna (price, currency) ou (None, None).
    """
    if not text:
        return None, None

    # Padrões: USD 485, US$ 485.00, 485 USD, R$ 485,00
    patterns = [
        r"USD?\s*\$?\s*([\d.,]+)",
        r"US\$\s*([\d.,]+)",
        r"([\d.,]+)\s*USD",
        r"R\$\s*([\d.,]+)",
        r"BRL\s*([\d.,]+)",
    ]
    for i, pat in enumerate(patterns):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1).replace(",", ".")
            try:
                price = float(raw)
                if 10 < price < 5_000_000:
                    currency = "BRL" if i >= 3 else "USD"
                    return price, currency
            except ValueError:
                pass
    return None, None


def _extract_volume_from_text(text: str) -> tuple[float | None, str | None]:
    """
    Extrai volume e unidade de texto livre.
    Ex: '12.500 MT', '300 mil MT mês', '70 mil tons'
    """
    if not text:
        return None, None

    # "X mil MT" ou "X mil tons"
    m = re.search(r"([\d.,]+)\s*mil\s*(MT|ton|tons|toneladas)?", text, re.IGNORECASE)
    if m:
        try:
            vol = float(m.group(1).replace(",", ".")) * 1000
            unit = "MT"
            return vol, unit
        except ValueError:
            pass

    # "X MT" direto
    m = re.search(r"([\d.,]+)\s*(MT|ton|tons|toneladas|sacas|containers?|ctnrs?)", text, re.IGNORECASE)
    if m:
        try:
            vol = float(m.group(1).replace(".", "").replace(",", "."))
            raw_unit = m.group(2).lower()
            unit = "SC" if "sac" in raw_unit else "MT"
            return vol, unit
        except ValueError:
            pass

    return None, None


def _get_existing_names(session: Session) -> set[str]:
    """Retorna set de names (JOB IDs) já existentes no banco."""
    rows = session.execute(
        __import__("sqlalchemy").text("SELECT name FROM deals WHERE name IS NOT NULL")
    ).fetchall()
    return {r[0] for r in rows}


# ── Parser das abas de Pipeline ───────────────────────────────────────────────

def _parse_pipeline_rows(soup: BeautifulSoup, source_file: str) -> list[dict]:
    """
    Lê tabela HTML de uma aba de pipeline e retorna lista de dicts com os campos.
    O schema esperado é:
      JOB | DATA ENTRADA | OFERTAouPEDIDO | GRUPO | SOLICITANTE | STATUS |
      PRODUTO | COMPRADOR | FORNECEDOR | VISUALIZAÇÃO RAPIDA | DOCs | ESPECIFICAÇÃO
    """
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    headers = []
    data_rows = []

    for i, row in enumerate(rows):
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        non_empty = [c for c in cells if c]

        # Linha de headers é a que contém "JOB"
        if not headers and "JOB" in cells:
            headers = cells
            continue

        if headers and len(non_empty) >= 2:
            # Monta dict por posição
            d = {headers[j]: cells[j] for j in range(min(len(headers), len(cells)))}
            data_rows.append(d)

    records = []
    for d in data_rows:
        job_id = d.get("JOB", "").strip()
        produto_raw = d.get("PRODUTO", "").strip()
        status_raw = d.get("STATUS", "").strip().lower()
        oferta_pedido = d.get("OFERTAouPEDIDO", "").strip().lower()
        grupo = d.get("GRUPO", "").strip()
        solicitante = d.get("SOLICITANTE", "").strip()
        comprador = d.get("COMPRADOR", "").strip()
        fornecedor = d.get("FORNECEDOR", "").strip()
        vis_rapida = d.get("VISUALIZAÇÃO RAPIDA", d.get("VISUALIZAÇÃO RÁPIDA", "")).strip()
        espec = d.get("ESPECIFICAÇÃO / WHATS", d.get("ESPECIFICAÇÃO/WHATS", "")).strip()

        if not produto_raw:
            continue  # Linha sem produto, pular

        commodity = _normalize_commodity(produto_raw)
        stage = STATUS_MAP.get(status_raw, "Lead Capturado")
        direcao = DIRECAO_MAP.get(oferta_pedido, "UNKNOWN")
        status_deal = "arquivado" if status_raw in ARCHIVED_STATUS else "ativo"

        # Extrai preço e volume do campo VISUALIZAÇÃO RAPIDA
        price, currency = _extract_price_from_text(vis_rapida or espec)
        volume, volume_unit = _extract_volume_from_text(vis_rapida or espec)

        # Incoterm
        incoterm_match = re.search(r"\b(FOB|CIF|CFR|FAS|DAP|DDP|EXW|ASWP)\b", vis_rapida or espec, re.IGNORECASE)
        incoterm = incoterm_match.group(1).upper() if incoterm_match else None

        assignee = _resolve_assignee(commodity)

        records.append({
            "name":           job_id if job_id else None,
            "commodity":      commodity,
            "direcao":        direcao,
            "stage":          stage,
            "status":         status_deal,
            "price":          price,
            "currency":       currency or "USD",
            "volume":         volume,
            "volume_unit":    volume_unit,
            "incoterm":       incoterm,
            "source_group":   grupo,
            "source_sender":  solicitante,
            "destination":    comprador if direcao == "BID" else None,
            "origin":         fornecedor if direcao == "ASK" else None,
            "notes":          vis_rapida[:500] if vis_rapida else espec[:500],
            "assignee":       assignee,
            "source_file":    source_file,
        })

    return records


# ── Parser da aba valores comuns ──────────────────────────────────────────────

def _parse_valores_comuns(soup: BeautifulSoup) -> list[dict]:
    """
    Aba de preços comparativos por broker.
    Schema: Produto | origem | Incoterm | x | x | x | Rokane | Alexandre | Raphael | Vasconcelos
    """
    table = soup.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    headers = []
    records = []

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        non_empty = [c for c in cells if c]

        if not headers and "Produto" in cells:
            headers = cells
            continue

        if not headers or len(non_empty) < 3:
            continue

        d = {headers[j]: cells[j] for j in range(min(len(headers), len(cells)))}
        produto_raw = d.get("Produto", "").strip()
        origem = d.get("origem", "").strip()
        incoterm_raw = d.get("Incoterm", "").strip().upper()

        if not produto_raw:
            continue

        commodity = _normalize_commodity(produto_raw)
        brokers = ["Rokane", "Alexandre", "Raphael", "Vasconcelos"]

        for broker in brokers:
            cell_text = d.get(broker, "").strip()
            if not cell_text:
                continue
            price, currency = _extract_price_from_text(cell_text)
            if not price:
                continue
            volume, volume_unit = _extract_volume_from_text(cell_text)

            records.append({
                "name":          None,  # Sem JOB ID
                "commodity":     commodity,
                "direcao":       "ASK",  # Brokers estão ofertando
                "stage":         "Lead Capturado",
                "status":        "ativo",
                "price":         price,
                "currency":      currency or "USD",
                "volume":        volume,
                "volume_unit":   volume_unit,
                "incoterm":      incoterm_raw or None,
                "origin":        origem or None,
                "source_group":  broker,
                "source_sender": broker,
                "assignee":      _resolve_assignee(commodity),
                "notes":         cell_text[:300],
                "source_file":   "valores comuns .html",
            })

    return records


# ── Parser do rokane.html ─────────────────────────────────────────────────────

def _parse_rokane(soup: BeautifulSoup) -> list[dict]:
    """
    Planilha de preços do Rokane. Formato matricial com produtos em colunas.
    Extrai: commodity, SPEC, ORIGIN, PRICE, TRIAL/MIN/MAX volume, INCOTERM.
    """
    table = soup.find("table")
    if not table:
        return []

    rows = [[td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            for row in table.find_all("tr")]

    # Localiza blocos de produto: linha que começa com "SPEC" ou "PRODUCT"
    # Cada produto ocupa colunas (ex: col 0-1 = IC45, col 3-4 = FRANGO, etc.)
    # Estratégia: varre todas as células em busca de padrões de preço/produto

    products = []
    current = {}

    for row in rows:
        for i, cell in enumerate(row):
            cell_up = cell.upper().strip()
            val = row[i + 1].strip() if i + 1 < len(row) else ""

            if not val:
                continue

            if cell_up in ("PRODUCT", "SPEC"):
                if current and current.get("commodity"):
                    products.append(current.copy())
                current = {"commodity": _normalize_commodity(val)}

            elif cell_up == "ORIGEN" or cell_up == "ORIGIN":
                current["origin"] = val

            elif cell_up == "PRICE":
                price, currency = _extract_price_from_text(val)
                current["price"] = price
                current["currency"] = currency or "USD"
                inco = re.search(r"\b(FOB|CIF|CFR|FAS|DAP|DDP|EXW|ASWP)\b", val, re.IGNORECASE)
                if inco:
                    current["incoterm"] = inco.group(1).upper()

            elif cell_up == "INCOTERM":
                current["incoterm"] = val.upper()

            elif cell_up in ("TRIAL", "CONTRACT MIN:", "CONTRACT MIN"):
                vol, unit = _extract_volume_from_text(val)
                if vol:
                    current.setdefault("volume", vol)
                    current.setdefault("volume_unit", unit or "MT")

    if current and current.get("commodity"):
        products.append(current)

    records = []
    for p in products:
        if not p.get("price"):
            continue
        records.append({
            "name":          None,
            "commodity":     p.get("commodity", "Indefinida"),
            "direcao":       "ASK",
            "stage":         "Lead Capturado",
            "status":        "ativo",
            "price":         p.get("price"),
            "currency":      p.get("currency", "USD"),
            "volume":        p.get("volume"),
            "volume_unit":   p.get("volume_unit", "MT"),
            "incoterm":      p.get("incoterm"),
            "origin":        p.get("origin"),
            "source_group":  "rokane",
            "source_sender": "Rokane/Fabrício",
            "assignee":      _resolve_assignee(p.get("commodity", "")),
            "notes":         f"Preço Rokane {p.get('currency','USD')} {p.get('price',0):.2f}/MT",
            "source_file":   "rokane.html",
        })

    return records


# ── Parser do FORNECEDORES.html ───────────────────────────────────────────────

def _parse_fornecedores(soup: BeautifulSoup, session: Session) -> int:
    """
    Salva fornecedores na tabela StrategicData como JSON Lines.
    Retorna quantidade inserida.
    """
    table = soup.find("table")
    if not table:
        return 0

    rows = table.find_all("tr")
    headers = []
    inserted = 0

    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        non_empty = [c for c in cells if c]

        if not headers and "PRODUTO" in cells:
            headers = cells
            continue

        if not headers or len(non_empty) < 2:
            continue

        d = {headers[j]: cells[j] for j in range(min(len(headers), len(cells)))}
        if not any(d.values()):
            continue

        # Anti-duplicata simples por CONTATO+PRODUTO
        contato = d.get("CONTATO", "")
        produto = d.get("PRODUTO", "")
        if not contato and not produto:
            continue

        row_data = StrategicData(
            sheet_name="FORNECEDORES",
            raw_json=json.dumps(d, ensure_ascii=False),
        )
        session.add(row_data)
        inserted += 1

    session.commit()
    return inserted


# ── Inserção de deals no banco ────────────────────────────────────────────────

def _insert_deals(records: list[dict], session: Session, existing_names: set[str]) -> tuple[int, int]:
    """
    Insere records como Deal, pulando duplicatas por name (JOB ID).
    Retorna (inseridos, pulados).
    """
    inserted = 0
    skipped = 0

    for r in records:
        name = r.get("name")
        if name and name in existing_names:
            skipped += 1
            continue

        deal = Deal(
            name=name or f"[{r['commodity']}] {r.get('source_sender','?')} - {r.get('source_file','?')}",
            commodity=r.get("commodity"),
            direcao=r.get("direcao", "UNKNOWN"),
            volume=r.get("volume"),
            volume_unit=r.get("volume_unit"),
            price=r.get("price"),
            currency=r.get("currency", "USD"),
            incoterm=r.get("incoterm"),
            origin=r.get("origin"),
            destination=r.get("destination"),
            stage=r.get("stage", "Lead Capturado"),
            status=r.get("status", "ativo"),
            risk_score=50,
            source_group=r.get("source_group"),
            source_sender=r.get("source_sender"),
            assignee=r.get("assignee"),
            notes=r.get("notes"),
        )
        session.add(deal)

        if name:
            existing_names.add(name)
        inserted += 1

    session.commit()
    return inserted, skipped


# ── Entry point principal ─────────────────────────────────────────────────────

def ingest_all_sheets(sheets_dir: Path = SHEETS_DIR) -> dict:
    """
    Roda a ingestão completa de todas as abas HTML.
    Retorna resumo com contagens por arquivo.
    """
    if not sheets_dir.exists():
        logger.error("Pasta %s não encontrada. Crie e copie os .html para ela.", sheets_dir)
        return {}

    engine = get_engine()
    create_tables()
    session_obj = __import__("sqlalchemy.orm", fromlist=["sessionmaker"]).sessionmaker(bind=engine)()

    existing_names = _get_existing_names(session_obj)
    logger.info("Deals existentes no banco: %d nomes registrados.", len(existing_names))

    summary: dict[str, dict] = {}

    # ── 1. PIPELINE SHEETS ─────────────────────────────────────────────────────
    for fname in PIPELINE_FILES:
        fpath = sheets_dir / fname
        if not fpath.exists():
            logger.warning("Arquivo não encontrado: %s", fpath)
            continue

        logger.info("Processando pipeline: %s", fname)
        soup = _read_html(fpath)
        if not soup:
            continue

        records = _parse_pipeline_rows(soup, fname)
        ins, skip = _insert_deals(records, session_obj, existing_names)
        summary[fname] = {"parsed": len(records), "inserted": ins, "skipped": skip}
        logger.info("  %s → %d registros | %d inseridos | %d pulados", fname, len(records), ins, skip)

    # ── 2. VALORES COMUNS ──────────────────────────────────────────────────────
    vc_path = sheets_dir / "valores comuns .html"
    if vc_path.exists():
        logger.info("Processando: valores comuns .html")
        soup = _read_html(vc_path)
        if soup:
            records = _parse_valores_comuns(soup)
            ins, skip = _insert_deals(records, session_obj, existing_names)
            summary["valores comuns .html"] = {"parsed": len(records), "inserted": ins, "skipped": skip}
            logger.info("  valores comuns .html → %d registros | %d inseridos", len(records), ins)

    # ── 3. ROKANE PRICE SHEET ──────────────────────────────────────────────────
    rk_path = sheets_dir / "rokane.html"
    if rk_path.exists():
        logger.info("Processando: rokane.html")
        soup = _read_html(rk_path)
        if soup:
            records = _parse_rokane(soup)
            ins, skip = _insert_deals(records, session_obj, existing_names)
            summary["rokane.html"] = {"parsed": len(records), "inserted": ins, "skipped": skip}
            logger.info("  rokane.html → %d registros | %d inseridos", len(records), ins)

    # ── 4. FORNECEDORES ────────────────────────────────────────────────────────
    fn_path = sheets_dir / "FORNECEDORES.html"
    if fn_path.exists():
        logger.info("Processando: FORNECEDORES.html (tabela StrategicData)")
        soup = _read_html(fn_path)
        if soup:
            n = _parse_fornecedores(soup, session_obj)
            summary["FORNECEDORES.html"] = {"inserted_strategic": n}
            logger.info("  FORNECEDORES.html → %d fornecedores salvos em StrategicData", n)

    session_obj.close()
    return summary


# ── CLI standalone ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    result = ingest_all_sheets()
    print("\n=== RESUMO DA INGESTÃO ===")
    total_deals = 0
    for fname, info in result.items():
        ins = info.get("inserted", info.get("inserted_strategic", 0))
        total_deals += ins
        print(f"  {fname}: {ins} inseridos | {info.get('skipped', 0)} pulados")
    print(f"\nTotal de registros novos: {total_deals}")
