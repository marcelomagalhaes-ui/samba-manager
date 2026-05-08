"""
scripts/scan_drive_inventory.py
================================
Samba Master Engine - TAREFA 1: Varredura Profunda + Inventario Documental.

1. Varre recursivamente a pasta comercial do Drive (COMERCIAL).
2. Classifica cada arquivo por tipo (LOI, ICPO, SCO, FCO, SPA, SBLC, RWA, Ficha).
3. Cria um novo Google Sheet dentro da pasta destino (SAMBA_AGENTS) com
   o inventario completo: [Cliente/Pasta | Nome | Tipo | Data | Link].

Output: imprime o ID + URL da planilha criada ao final.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from googleapiclient.discovery import build

from services.google_drive import drive_manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ScanInventory")

# --------------------------------------------------------------------------
# Configuracao
# --------------------------------------------------------------------------

SOURCE_FOLDER_ID = "1k0uKPg7Xyq8MyI8KI1bRKzR-Bow_41B5"   # COMERCIAL
TARGET_FOLDER_ID = "1NLomT9f0zqDiAQC8j92V2UeSaN5bnHSJ"   # SAMBA_AGENTS

# Padroes para classificacao (case-insensitive, primeiro match vence).
DOC_PATTERNS: list[tuple[str, list[str]]] = [
    ("LOI",              ["loi", "letter of intent"]),
    ("ICPO",             ["icpo", "irrevocable corporate purchase"]),
    ("SCO",              ["sco ", "sco_", "sco-", "soft corporate offer"]),
    ("FCO",              ["fco ", "fco_", "fco-", "full corporate offer"]),
    ("SPA",              ["spa ", "spa_", "spa-", "sales and purchase", "sale and purchase"]),
    ("SBLC",             ["sblc", "standby letter"]),
    ("RWA",              ["rwa", "ready willing"]),
    ("Ficha de Cadastro",["ficha_cadastro", "ficha de cadastro", "ficha cadastro"]),
]

# MimeTypes consideradas "lixo" ou auxiliares (marcadas mas reportadas).
NOISE_MIMES = {
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "text/html",
    "application/xhtml+xml",
}


# --------------------------------------------------------------------------
# Classificacao
# --------------------------------------------------------------------------

def classify(filename: str, mime: str) -> str:
    """Retorna o tipo do documento, 'Ruido' para .xlsx/.html, ou 'Outro'."""
    if mime in NOISE_MIMES:
        return "Ruido"
    low = filename.lower()
    for label, patterns in DOC_PATTERNS:
        if any(p in low for p in patterns):
            return label
    return "Outro"


# --------------------------------------------------------------------------
# Varredura recursiva
# --------------------------------------------------------------------------

def list_children(service, folder_id: str) -> list[dict[str, Any]]:
    """Lista TODOS os filhos de uma pasta (arquivos + subpastas), paginado."""
    children: list[dict[str, Any]] = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"
    while True:
        resp = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType, createdTime, webViewLink, parents)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        children.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return children


def walk(service, root_id: str, root_name: str) -> list[dict[str, Any]]:
    """BFS da arvore. Retorna lista flat de arquivos (sem pastas).

    Cada item ganha `cliente_path`: cadeia de pastas desde a raiz COMERCIAL.
    """
    inventory: list[dict[str, Any]] = []
    # stack = [(folder_id, path_prefix)]
    stack: list[tuple[str, str]] = [(root_id, root_name)]
    visited_folders = 0

    while stack:
        folder_id, path_prefix = stack.pop(0)
        visited_folders += 1
        try:
            kids = list_children(service, folder_id)
        except Exception as e:
            logger.error("Falha listando %s (%s): %s", path_prefix, folder_id, e)
            continue

        for kid in kids:
            if kid["mimeType"] == "application/vnd.google-apps.folder":
                sub_path = f"{path_prefix} / {kid['name']}"
                stack.append((kid["id"], sub_path))
            else:
                inventory.append({
                    "cliente_path": path_prefix,
                    "file_id": kid["id"],
                    "name": kid["name"],
                    "mime": kid["mimeType"],
                    "created": kid.get("createdTime", ""),
                    "link": kid.get("webViewLink", ""),
                })

        # Pequeno throttle amigavel para a API.
        if visited_folders % 20 == 0:
            time.sleep(0.2)

    logger.info("Varredura: %s pastas visitadas, %s arquivos coletados.",
                visited_folders, len(inventory))
    return inventory


# --------------------------------------------------------------------------
# Criacao do Sheet destino
# --------------------------------------------------------------------------

def create_inventory_sheet(drive_svc, sheets_svc, target_folder: str, title: str) -> str:
    """Cria a planilha ja dentro da pasta target e retorna o spreadsheetId."""
    meta = {
        "name": title,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [target_folder],
    }
    created = drive_svc.files().create(
        body=meta,
        fields="id, webViewLink",
        supportsAllDrives=True,
    ).execute()
    sheet_id = created["id"]
    logger.info("Planilha criada: %s  (%s)", created.get("webViewLink"), sheet_id)
    return sheet_id


def write_inventory(sheets_svc, sheet_id: str, rows: list[list[str]]) -> None:
    """Escreve cabecalho + linhas no Sheet (aba default)."""
    header = ["Cliente/Pasta", "Nome do Arquivo", "Tipo de Documento",
              "Data de Criacao", "Link Direto"]
    body = {"values": [header] + rows}
    sheets_svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="A1",
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()
    logger.info("Escritas %s linhas no Sheet.", len(rows))


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> None:
    if not drive_manager.creds:
        logger.error("drive_manager sem credenciais. Abortando.")
        sys.exit(2)

    drive_svc = build("drive", "v3", credentials=drive_manager.creds)
    sheets_svc = build("sheets", "v4", credentials=drive_manager.creds)

    # Passo 1: varre
    logger.info("Iniciando varredura em COMERCIAL (%s)...", SOURCE_FOLDER_ID)
    inventory = walk(drive_svc, SOURCE_FOLDER_ID, "COMERCIAL")

    # Passo 2: classifica + monta linhas
    stats: dict[str, int] = {}
    rows: list[list[str]] = []
    for item in inventory:
        tipo = classify(item["name"], item["mime"])
        stats[tipo] = stats.get(tipo, 0) + 1
        data_fmt = ""
        if item["created"]:
            try:
                data_fmt = datetime.fromisoformat(
                    item["created"].replace("Z", "+00:00")
                ).strftime("%d/%m/%Y %H:%M")
            except Exception:
                data_fmt = item["created"]
        rows.append([
            item["cliente_path"],
            item["name"],
            tipo,
            data_fmt,
            item["link"],
        ])

    # Ordena por cliente_path, depois tipo
    rows.sort(key=lambda r: (r[0], r[2], r[1]))
    logger.info("Stats por tipo: %s", json.dumps(stats, ensure_ascii=False))

    # Passo 3: cria sheet + escreve
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    title = f"Inventario Documental Samba - {stamp}"
    sheet_id = create_inventory_sheet(drive_svc, sheets_svc, TARGET_FOLDER_ID, title)
    write_inventory(sheets_svc, sheet_id, rows)

    # Salva um snapshot JSON para a TAREFA 3 (badges no Kanban).
    snapshot_path = ROOT / "data" / "doc_inventory_snapshot.json"
    snapshot_path.parent.mkdir(exist_ok=True)
    snapshot_path.write_text(
        json.dumps({"rows": rows, "stats": stats, "sheet_id": sheet_id,
                    "generated_at": datetime.now().isoformat()},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Snapshot salvo em %s", snapshot_path)

    print("\n=== RESULTADO ===")
    print(f"Sheet ID : {sheet_id}")
    print(f"URL      : https://docs.google.com/spreadsheets/d/{sheet_id}/edit")
    print(f"Titulo   : {title}")
    print(f"Linhas   : {len(rows)}")
    print(f"Stats    : {stats}")


if __name__ == "__main__":
    main()
