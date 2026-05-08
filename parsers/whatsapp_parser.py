"""
Parser de conversas exportadas do WhatsApp (.txt)
Formato: DD/MM/AAAA HH:MM - Remetente: Mensagem
"""
import re
import json
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class WhatsAppMessage:
    """Uma mensagem individual do WhatsApp."""
    timestamp: datetime
    sender: str
    content: str
    group_name: str = ""
    is_media: bool = False
    is_system: bool = False
    # Campos extraídos pelo Agente Extrator
    commodity: Optional[str] = None
    price: Optional[float] = None
    currency: Optional[str] = None
    volume: Optional[float] = None
    volume_unit: Optional[str] = None
    incoterm: Optional[str] = None
    location: Optional[str] = None
    has_quote: bool = False

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


# Regex para linha de mensagem WhatsApp
# Formato: DD/MM/AAAA HH:MM - Sender: Message
# \u200e = LTR mark que o WhatsApp insere antes de nomes em alguns exports
MSG_PATTERN = re.compile(
    r"^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})\s+-\s+\u200e?(.+?):\s(.+)$"
)

# Regex para mensagens de sistema (sem ":")
SYSTEM_PATTERN = re.compile(
    r"^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})\s+-\s+\u200e?(.+)$"
)

# Padrões de preço
PRICE_PATTERNS = [
    re.compile(r"(?:USD|US\$|U\$)\s*([\d.,]+)", re.IGNORECASE),
    re.compile(r"R\$\s*([\d.,]+)", re.IGNORECASE),
    re.compile(r"([\d.,]+)\s*(?:usd|dol[aá]r)", re.IGNORECASE),
    re.compile(r"([\d.,]+)\s*/\s*(?:mt|ton|sc|kg|l)", re.IGNORECASE),
]

# Padrões de volume
VOLUME_PATTERNS = [
    re.compile(r"([\d.,]+)\s*(?:mil|k)\s*(?:mt|ton|toneladas?)", re.IGNORECASE),
    re.compile(r"([\d.,]+)\s*(?:mt|ton|toneladas?)", re.IGNORECASE),
    re.compile(r"([\d.,]+)\s*(?:sc|sacas?)", re.IGNORECASE),
    re.compile(r"([\d.,]+)\s*(?:m[³3]|litros?)", re.IGNORECASE),
]

# Incoterms
INCOTERM_PATTERN = re.compile(
    r"\b(FOB|CIF|FAS|CFR|CIP|DAP|DDP|FCA|EXW|ASWP)\b", re.IGNORECASE
)

# Commodities keywords (expandir conforme necessário)
COMMODITY_KEYWORDS = {
    "Soja": ["soja", "soy", "soybeans", "soybean"],
    "Açúcar": ["açúcar", "sugar", "icumsa", "vhp"],
    "Milho": ["milho", "corn", "ddgs"],
    "Arroz": ["arroz", "rice", "paddy"],
    "Café": ["café", "coffee", "arábica", "arabica", "conilon"],
    "Algodão": ["algodão", "algodao", "cotton", "pluma"],
    "Etanol": ["etanol", "ethanol"],
    "Frango": ["frango", "chicken", "pé de frango", "chicken paw", "chicken feet"],
    "Boi": ["boi", "beef", "carne bovina"],
    "Porco": ["porco", "pork", "suíno"],
    "Trigo": ["trigo", "wheat"],
    "Feijão": ["feijão", "feijao", "beans"],
    "Madeira": ["madeira", "timber", "eucalipto", "pinus", "wood chips", "plywood"],
    "Minerais": ["ferro", "iron", "lítio", "lithium", "manganês", "manganese"],
    "Óleo Vegetal": ["óleo", "oil", "palm", "sunflower"],
    "Cacau": ["cacau", "cocoa"],
    "Prata": ["prata", "silver"],
}


def parse_price(text: str) -> tuple[Optional[float], Optional[str]]:
    """Extrai preço e moeda de um texto."""
    for pattern in PRICE_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1).replace(".", "").replace(",", ".")
            try:
                price = float(raw)
                currency = "USD" if "usd" in text.lower() or "us$" in text.lower() or "u$" in text.lower() else "BRL"
                return price, currency
            except ValueError:
                continue
    return None, None


def parse_volume(text: str) -> tuple[Optional[float], Optional[str]]:
    """Extrai volume e unidade de um texto."""
    for pattern in VOLUME_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1).replace(".", "").replace(",", ".")
            try:
                vol = float(raw)
                # Detectar se é "mil"
                if re.search(r"\bmil\b", text[m.start():m.end()+10], re.IGNORECASE):
                    vol *= 1000
                unit = "MT"
                if re.search(r"\b(sc|sacas?)\b", text[m.start():m.end()+10], re.IGNORECASE):
                    unit = "SC"
                elif re.search(r"\b(m[³3]|litros?)\b", text[m.start():m.end()+10], re.IGNORECASE):
                    unit = "M3"
                return vol, unit
            except ValueError:
                continue
    return None, None


def detect_commodity(text: str) -> Optional[str]:
    """Detecta a commodity mencionada no texto."""
    text_lower = text.lower()
    for commodity, keywords in COMMODITY_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                return commodity
    return None


def detect_incoterm(text: str) -> Optional[str]:
    """Detecta incoterm no texto."""
    m = INCOTERM_PATTERN.search(text)
    return m.group(1).upper() if m else None


def parse_chat_file(filepath: str | Path) -> list[WhatsAppMessage]:
    """
    Parseia um arquivo .txt exportado do WhatsApp.
    Retorna lista de WhatsAppMessage.
    """
    filepath = Path(filepath)
    # Suporta tanto nomes com underscore quanto com espaço
    stem = filepath.stem
    for prefix in ("Conversa_do_WhatsApp_com_", "Conversa do WhatsApp com "):
        if stem.startswith(prefix):
            stem = stem[len(prefix):]
            break
    group_name = stem.replace("_", " ").strip()

    messages: list[WhatsAppMessage] = []
    current_msg: Optional[WhatsAppMessage] = None

    # Tenta utf-8-sig primeiro (com BOM), depois utf-8, depois latin-1
    raw_lines: list[str] = []
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                raw_lines = f.readlines()
            break
        except UnicodeDecodeError:
            continue

    for line in raw_lines:
            line = line.rstrip("\n")
            if not line:
                continue

            # Tentar match de mensagem normal
            m = MSG_PATTERN.match(line)
            if m:
                # Salvar mensagem anterior se existir
                if current_msg:
                    _enrich_message(current_msg)
                    messages.append(current_msg)

                date_str, time_str, sender, content = m.groups()
                try:
                    ts = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
                except ValueError:
                    continue

                is_media = content.strip() == "<Mídia oculta>"
                sender = sender.strip().rstrip(" -")

                current_msg = WhatsAppMessage(
                    timestamp=ts,
                    sender=sender,
                    content=content.strip(),
                    group_name=group_name,
                    is_media=is_media,
                    is_system=False,
                )
                continue

            # Tentar match de mensagem de sistema
            sm = SYSTEM_PATTERN.match(line)
            if sm and not MSG_PATTERN.match(line):
                if current_msg:
                    _enrich_message(current_msg)
                    messages.append(current_msg)

                date_str, time_str, content = sm.groups()
                try:
                    ts = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
                except ValueError:
                    continue

                current_msg = WhatsAppMessage(
                    timestamp=ts,
                    sender="SYSTEM",
                    content=content.strip(),
                    group_name=group_name,
                    is_system=True,
                )
                continue

            # Linha de continuação (multiline message)
            if current_msg and not current_msg.is_system:
                current_msg.content += "\n" + line

    # Não esquecer a última mensagem
    if current_msg:
        _enrich_message(current_msg)
        messages.append(current_msg)

    return messages


def _enrich_message(msg: WhatsAppMessage):
    """Enriquece mensagem com dados extraídos (commodity, preço, volume, incoterm)."""
    if msg.is_system or msg.is_media:
        return

    text = msg.content
    msg.commodity = detect_commodity(text)
    msg.price, msg.currency = parse_price(text)
    msg.volume, msg.volume_unit = parse_volume(text)
    msg.incoterm = detect_incoterm(text)
    msg.has_quote = any([msg.price, msg.volume, msg.commodity])


def parse_all_chats(directory: str | Path) -> list[WhatsAppMessage]:
    """Parseia todos os arquivos .txt de WhatsApp em um diretório."""
    directory = Path(directory)
    all_messages = []

    # Aceita tanto nomes com underscores quanto com espaços (formato WhatsApp Android/iOS)
    files: list[Path] = []
    for pattern in ("Conversa_do_WhatsApp_com_*.txt", "Conversa do WhatsApp com *.txt"):
        files.extend(directory.glob(pattern))
    # Fallback: qualquer .txt se nenhum padrão bater
    if not files:
        files = list(directory.glob("*.txt"))
    files = sorted(set(files))

    for f in files:
        msgs = parse_chat_file(f)
        all_messages.extend(msgs)
        print(f"  ✓ {f.name}: {len(msgs)} mensagens ({sum(1 for m in msgs if m.has_quote)} com cotação)")
    all_messages.sort(key=lambda m: m.timestamp)
    return all_messages


def export_quotes_csv(messages: list[WhatsAppMessage], output: str | Path):
    """Exporta apenas mensagens com cotações para CSV."""
    import csv
    quotes = [m for m in messages if m.has_quote]
    output = Path(output)

    with open(output, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Data", "Hora", "Grupo", "Remetente", "Commodity",
            "Preço", "Moeda", "Volume", "Unidade", "Incoterm", "Mensagem"
        ])
        for q in quotes:
            writer.writerow([
                q.timestamp.strftime("%Y-%m-%d"),
                q.timestamp.strftime("%H:%M"),
                q.group_name,
                q.sender,
                q.commodity or "",
                q.price or "",
                q.currency or "",
                q.volume or "",
                q.volume_unit or "",
                q.incoterm or "",
                q.content[:200].replace("\n", " "),
            ])
    print(f"  📊 {len(quotes)} cotações exportadas para {output}")
    return quotes


def export_messages_json(messages: list[WhatsAppMessage], output: str | Path):
    """Exporta todas as mensagens para JSON."""
    output = Path(output)
    data = [m.to_dict() for m in messages if not m.is_system]
    with open(output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 {len(data)} mensagens exportadas para {output}")


# ── CLI standalone ──
if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
    print(f"\n🎷 SAMBA EXPORT — WhatsApp Parser")
    print(f"{'='*50}")
    print(f"Diretório: {target}\n")

    msgs = parse_all_chats(target)
    print(f"\n{'='*50}")
    print(f"Total: {len(msgs)} mensagens")
    print(f"Com cotação: {sum(1 for m in msgs if m.has_quote)}")
    print(f"Grupos: {len(set(m.group_name for m in msgs))}")
    print(f"Período: {msgs[0].timestamp.date()} a {msgs[-1].timestamp.date()}")

    export_quotes_csv(msgs, "data/processed/cotacoes.csv")
    export_messages_json(msgs, "data/processed/mensagens.json")
