"""Diagnostico da pasta _PRE LOI (templates de Price Indication)."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)
except Exception:
    pass

from services.google_drive import DriveManager

FOLDER_ID = "1EU0KkSzHKhxqOlp3XGC-xvs6Xzvf_XTh"   # _PRE LOI
FILE_ID   = "1W57WR2h3Vi3jD-ZW4rY7iJjPA1H9IW2nBb8hpObQnD4"   # GDoc novo

drive = DriveManager()
if not drive.service:
    print("Drive nao autenticado.")
    sys.exit(1)

print(f"\n[1] Listando arquivos na pasta {FOLDER_ID}:\n")
try:
    results = drive.service.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed = false",
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
        pageSize=50,
    ).execute()
    files = results.get("files", [])
    if not files:
        print("  Nenhum arquivo encontrado.")
    for f in files:
        print(f"  - {f['name']}  ({f['mimeType']})  id={f['id']}")
except Exception as e:
    print(f"  ERRO listagem: {e}")

print(f"\n[2] Acesso direto ao file ID {FILE_ID}:\n")
try:
    meta = drive.service.files().get(
        fileId=FILE_ID,
        fields="id,name,mimeType,parents",
        supportsAllDrives=True,
    ).execute()
    print(f"  OK: {meta}")
    b = drive.fetch_as_docx_bytes(meta)
    print(f"  bytes baixados: {len(b) if b else 0}")
except Exception as e:
    print(f"  ERRO acesso direto: {e}")
