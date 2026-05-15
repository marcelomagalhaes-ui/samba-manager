"""End-to-end: chama process_price_indication com dados minimos."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)
except Exception:
    pass

from agents.cotacao_agent import process_price_indication, _PI_LAST_LOAD_ERRORS

payload = {
    "document_type":  "PRICE_INDICATION",
    "template_name":  "2_PRICE_PREQUOTATION_SOY_SUGAR_CORN.docx",
    "output_format":  "pdf",
    "dry_run":        True,   # nao faz upload
    "dynamic_fields": {
        "COMODITIE_TYPE":     "SOYBEAN",
        "CITY":               "Main Port",
        "COUNTRY":            "CHINA",
        "DD/MM/YYYY":         "15/05/2026",
        "MM":                 "05",
        "YYYY":               "2026",
        "FIRST NAME Company": "COFCO",
        "FULL NAME Company":  "COFCO INTERNATIONAL",
        "PORTO":              "Paranagua",
    },
    "financial_fields": {
        "PRICE BASIS":            "450,00",
        "BASIS REFERENCIA PORTO": "-30,00",
        "PRICE FOB":              "420,00",
        "PRICE FREIGHT":          "42,00",
        "FINAL_PRICE":            "462,00",
        "COMISSION_CONTRACT":     "250.000,00",
        "QUANTITY_MT":            "50.000",
    },
}

result = process_price_indication(payload)
print("\n=== RESULTADO ===")
for k, v in result.items():
    if k == "file_bytes" and v:
        print(f"  {k}: <{len(v)} bytes>")
    else:
        print(f"  {k}: {v}")
print("\n=== LAST LOAD ERRORS ===")
for e in _PI_LAST_LOAD_ERRORS:
    print(f"  - {e}")

# Salva docx e extrai texto para verificar substituicoes
if result.get("file_bytes"):
    out = ROOT / "temp" / "diag_pi_output.docx"
    out.parent.mkdir(exist_ok=True)
    out.write_bytes(result["file_bytes"])
    print(f"\n  docx salvo em: {out}")

    # Lista marcadores que SOBRARAM
    import zipfile, re as _re
    with zipfile.ZipFile(out) as z:
        with z.open("word/document.xml") as f:
            xml = f.read().decode("utf-8", errors="ignore")
    # Remove tags XML para extrair texto puro
    text = _re.sub(r"<[^>]+>", " ", xml)
    text = _re.sub(r"\s+", " ", text)
    # Acha qualquer coisa entre { e } ou padroes "(Port}"
    remaining = _re.findall(r"\{[^}]{1,80}\}", text)
    typo_port = "(Port}" in text
    print(f"\n=== MARCADORES NAO SUBSTITUIDOS ===")
    for m in sorted(set(remaining)):
        print(f"  - {m}")
    if typo_port:
        print(f"  - (Port}}  [typo do template]")
    if not remaining and not typo_port:
        print("  (nenhum - tudo substituido!)")
