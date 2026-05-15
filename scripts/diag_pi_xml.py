"""Inspeciona como FINAL_PRICE e PRICE FOB aparecem no XML do template."""
import sys
import zipfile
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)
except Exception:
    pass

from services.google_drive import DriveManager

drive = DriveManager()
meta = drive.service.files().get(
    fileId="1W57WR2h3Vi3jD-ZW4rY7iJjPA1H9IW2nBb8hpObQnD4",
    fields="id,name,mimeType",
    supportsAllDrives=True,
).execute()
b = drive.fetch_as_docx_bytes(meta)

out = ROOT / "temp" / "template_raw.docx"
out.parent.mkdir(exist_ok=True)
out.write_bytes(b)

# Extrai document.xml
import io
with zipfile.ZipFile(io.BytesIO(b)) as z:
    xml = z.read("word/document.xml").decode("utf-8", errors="ignore")

# Procura trechos com FINAL_PRICE e FOB
import re as _re
for needle in ["FINAL_PRICE", "FOB", "FREIGHT", "BASIS", "Port"]:
    print(f"\n=== Trechos com '{needle}' ===")
    for m in _re.finditer(_re.escape(needle), xml):
        start = max(0, m.start() - 250)
        end   = min(len(xml), m.end() + 100)
        snippet = xml[start:end]
        print(f"  ...{snippet}...")
        print("  ---")
