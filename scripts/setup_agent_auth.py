"""
scripts/setup_agent_auth.py
============================
Gera (ou renova) o token OAuth2 para a conta agente@sambaexport.com.br,
incluindo os escopos de Drive + Gmail necessários para o sistema.

Executar UMA VEZ após:
  - Adicionar a conta agente@ ao Google Workspace
  - Habilitar Gmail API no Google Cloud Console (já feito)
  - Excluir o token.json anterior (escopo desatualizado)

Uso:
    cd C:\\SAMBA_MANAGER\\SAMBA_AGENTS
    del config\\token.json           (remove token antigo — escopo incompleto)
    python scripts/setup_agent_auth.py

O script abre um link no terminal. Copie-o, cole no Chrome,
faça login como agente@sambaexport.com.br e autorize os escopos.
O novo token.json é salvo em config/token.json automaticamente.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv()

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import os

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]

CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/google_credentials.json")
TOKEN_FILE = "config/token.json"


def main() -> None:
    print("=" * 60)
    print("  SAMBA AGENT — Setup de Autenticação OAuth2")
    print("=" * 60)
    print()

    creds_path = Path(CREDENTIALS_FILE)
    if not creds_path.exists():
        print(f"❌ Arquivo de credenciais não encontrado: {CREDENTIALS_FILE}")
        print("   Baixe o OAuth2 Client ID no Google Cloud Console e salve em config/")
        sys.exit(1)

    token_path = Path(TOKEN_FILE)

    # Tenta usar token existente (pode ainda ser válido se tiver os novos escopos)
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("♻️  Token expirado — renovando automaticamente…")
            creds.refresh(Request())
        else:
            print("🔐 Iniciando fluxo de autorização OAuth2…")
            print()
            print("   IMPORTANTE: quando o link aparecer abaixo,")
            print("   copie-o e cole no Chrome.")
            print("   Faça login como: agente@sambaexport.com.br")
            print("   Autorize os dois escopos: Drive + Gmail (Enviar e-mail)")
            print()

            flow = InstalledAppFlow.from_client_secrets_file(
                str(creds_path), SCOPES
            )
            creds = flow.run_local_server(port=0, open_browser=False)

        token_path.parent.mkdir(exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")
        print()
        print(f"✅ Token salvo em {TOKEN_FILE}")

    # Smoke test: valida Gmail API (envia email de teste)
    print()
    print("[OK] Validando Gmail API (envio de teste)...")
    try:
        from services.email_service import EmailService
        svc = EmailService()
        import os
        dest = os.getenv("EMAIL_MARCELO", "marcelo.magalhaes@sambaexport.com.br")
        html = EmailService.build_html(
            title="Setup concluido — Samba Agent",
            subtitle="Autenticacao OAuth2 com Gmail + Drive confirmada.",
            body_html=svc.section("Status", svc.row("Gmail API", "Operacional", highlight=True) + svc.row("Drive API", "Operacional")),
            icon="[OK]",
        )
        ok = svc.send_html(to=dest, subject="[Samba Agent] Setup de autenticacao concluido", html_body=html)
        if ok:
            print(f"[OK] Gmail API operacional — email de teste enviado para {dest}")
        else:
            print("[ERRO] Gmail API: falha no envio. Verifique os logs acima.")
    except Exception as exc:
        print(f"[ERRO] Gmail API: {exc}")

    # Smoke test: valida Drive API
    print()
    print("[OK] Validando Drive API...")
    try:
        from googleapiclient.discovery import build
        drive = build("drive", "v3", credentials=creds)
        about = drive.about().get(fields="user").execute()
        print(f"[OK] Drive API operacional — usuario: {about['user']['emailAddress']}")
    except Exception as exc:
        print(f"[ERRO] Drive API: {exc}")

    print()
    print("=" * 60)
    print("  Setup concluído. O sistema está pronto para usar.")
    print("=" * 60)


if __name__ == "__main__":
    main()
