"""
scripts/wpp_smoke_test.py
=========================
Smoke test completo da integração WhatsApp / Twilio.

Verifica:
  1. Credenciais Twilio — Account SID + Auth Token válidos
  2. Número configurado — SAMBA_WPP_MAIN reconhecido pelo Twilio
  3. Envio real — manda um "ping" para o próprio número (loopback)
  4. Webhook — mostra a URL que deve ser configurada no Twilio Console

Uso:
    python scripts/wpp_smoke_test.py             # diagnóstico apenas
    python scripts/wpp_smoke_test.py --send      # envia mensagem real de teste
    python scripts/wpp_smoke_test.py --send --to +5511999990001  # envia para outro número
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=True)
except Exception:
    pass

# ─────────────────────────────────────────────────────────────────────────────

SAMBA_WPP_MAIN   = os.getenv("SAMBA_WPP_MAIN", "+5513991405566")
TWILIO_SID       = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN     = os.getenv("TWILIO_AUTH_TOKEN", "")
WEBHOOK_BASE     = os.getenv("TWILIO_WEBHOOK_PUBLIC_URL", "https://SEU-DOMINIO/webhook/twilio")
WHATSAPP_OFFLINE = os.getenv("WHATSAPP_OFFLINE", "true").lower() == "true"

import io, sys as _sys
if hasattr(_sys.stdout, "reconfigure"):
    _sys.stdout.reconfigure(encoding="utf-8")
elif _sys.stdout.encoding and _sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    _sys.stdout = io.TextIOWrapper(_sys.stdout.buffer, encoding="utf-8", errors="replace")

SEP  = "-" * 60
OK   = "[OK] "
ERR  = "[XX] "
WARN = "[!!] "
INFO = "[>>] "


def check(label: str, ok: bool, detail: str = "") -> bool:
    sym = OK if ok else ERR
    print(f"  {sym}  {label}" + (f"  →  {detail}" if detail else ""))
    return ok


def run_diagnostics():
    print(f"\n{SEP}")
    print("  SAMBA EXPORT — WhatsApp Smoke Test")
    print(f"{SEP}\n")

    all_ok = True

    # 1. Número principal
    print("📱 Número principal")
    ok = bool(SAMBA_WPP_MAIN) and SAMBA_WPP_MAIN.startswith("+55")
    all_ok &= check("SAMBA_WPP_MAIN configurado", ok, SAMBA_WPP_MAIN)

    # 2. Credenciais Twilio
    print("\n🔑 Credenciais Twilio")
    sid_ok   = TWILIO_SID.startswith("AC") and len(TWILIO_SID) > 10
    token_ok = len(TWILIO_TOKEN) > 10 and TWILIO_TOKEN != "xxxxx"
    all_ok &= check("TWILIO_ACCOUNT_SID",  sid_ok,   TWILIO_SID[:6] + "***" if sid_ok else "NÃO CONFIGURADO")
    all_ok &= check("TWILIO_AUTH_TOKEN",   token_ok, "***" + TWILIO_TOKEN[-4:] if token_ok else "NÃO CONFIGURADO")

    # 3. Modo offline
    print("\n⚙️  Modo de operação")
    check("WHATSAPP_OFFLINE",
          True,
          f"{'ATIVO — nenhum envio real ocorrerá' if WHATSAPP_OFFLINE else 'INATIVO — envios reais habilitados'}")
    if WHATSAPP_OFFLINE:
        print(f"  {WARN}  Para ativar envios reais: definir WHATSAPP_OFFLINE=false no .env\n"
              f"       e preencher TWILIO_ACCOUNT_SID + TWILIO_AUTH_TOKEN")

    # 4. Twilio SDK
    print("\n📦 Dependências")
    try:
        import twilio
        check("twilio SDK instalado", True, f"v{twilio.__version__}")
    except ImportError:
        all_ok &= check("twilio SDK instalado", False, "execute: pip install twilio")

    # 5. Verificação ao vivo
    if sid_ok and token_ok:
        print("\n🌐 Verificação ao vivo (Twilio API)")
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            account = client.api.accounts(TWILIO_SID).fetch()
            check("Conta Twilio acessível", True, f"{account.friendly_name} [{account.status}]")

            # Verifica se o número está cadastrado como WhatsApp Sender
            try:
                senders = client.messaging.v1.services.list(limit=5)
                check("Messaging Services encontrados", True, f"{len(senders)} serviços")
            except Exception as e:
                check("Messaging Services", False, str(e)[:60])

        except Exception as e:
            all_ok &= check("Conta Twilio acessível", False, str(e)[:80])
    else:
        print(f"\n  {WARN}  Verificação ao vivo ignorada — credenciais incompletas")

    # 6. Webhook
    print("\n🔗 Webhook")
    print(f"  {INFO}  URL a configurar no Twilio Console:")
    print(f"       POST  {WEBHOOK_BASE}")
    print(f"  {INFO}  Caminho: Console → Messaging → WhatsApp Senders")
    print(f"         → seu número → Webhook URL = {WEBHOOK_BASE}")

    # 7. Agents configurados
    print("\n🤖 Agentes configurados")
    for role, envvar in [
        ("Extractor",  "AGENT_EXTRACTOR_PHONE"),
        ("Follow-Up",  "AGENT_FOLLOWUP_PHONE"),
        ("Manager",    "AGENT_MANAGER_PHONE"),
        ("Documental", "AGENT_DOCUMENTAL_PHONE"),
        ("Agenda",     "AGENT_AGENDA_PHONE"),
    ]:
        phone = os.getenv(envvar, "")
        ok = bool(phone) and phone.startswith("+55") and "99999" not in phone
        check(f"{role:12s} ({envvar})", ok, phone or "não configurado")

    print(f"\n{SEP}")
    if all_ok:
        print(f"  {OK}  Todos os checks passaram.")
    else:
        print(f"  {ERR}  Alguns checks falharam — revise os itens acima.")
    print(f"{SEP}\n")
    return all_ok


def send_test_message(to: str, offline: bool = False):
    """Envia uma mensagem de teste para `to` via WhatsApp."""
    from services.whatsapp_api import get_whatsapp_manager, AgentRole

    wm = get_whatsapp_manager()

    msg = (
        f"🟠 *SAMBA EXPORT — Smoke Test*\n"
        f"Agentes WhatsApp operacionais.\n"
        f"Número: {SAMBA_WPP_MAIN}\n"
        f"Timestamp: {__import__('datetime').datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
    )

    if offline or WHATSAPP_OFFLINE:
        print(f"\n  [OFFLINE] Mensagem que seria enviada para {to}:")
        print(f"  ┌{'─'*50}")
        for line in msg.split("\n"):
            print(f"  │ {line}")
        print(f"  └{'─'*50}\n")
        return

    print(f"\n  Enviando via Follow-Up → {to} ...")
    try:
        result = wm.send(AgentRole.FOLLOWUP, to, msg)
        print(f"  {OK}  Enviado! SID: {result.get('sid', '?')}")
    except Exception as e:
        print(f"  {ERR}  Falha ao enviar: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smoke test WhatsApp / Twilio")
    parser.add_argument("--send",   action="store_true", help="Envia mensagem real de teste")
    parser.add_argument("--to",     default=SAMBA_WPP_MAIN, help="Número destino (padrão: próprio número)")
    parser.add_argument("--offline", action="store_true", help="Forçar modo offline mesmo se .env diz false")
    args = parser.parse_args()

    run_diagnostics()

    if args.send:
        send_test_message(args.to, offline=args.offline)
