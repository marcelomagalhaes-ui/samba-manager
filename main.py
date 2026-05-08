"""
🎷 SAMBA EXPORT CONTROL DESK
============================
Plataforma de Agentes de IA para Gestão de Commodities

Uso:
    python main.py parse        # Parsear chats do WhatsApp
    python main.py init-db      # Criar banco de dados
    python main.py import       # Importar msgs parseadas no banco
    python main.py stats        # Estatísticas dos dados
    python main.py dashboard    # Abrir dashboard Streamlit
"""
import sys
import json
from pathlib import Path
from datetime import datetime

# Garantir que o diretório raiz está no path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def cmd_parse():
    """Parsear todos os chats WhatsApp da pasta data/raw/."""
    from parsers.whatsapp_parser import parse_all_chats, export_quotes_csv, export_messages_json

    raw_dir = ROOT / "data" / "raw"
    
    # --- COMPLEMENTO ADICIONADO AQUI ---
    # Verifica a pasta oficial de backups. Se existir, assume ela como diretório principal.
    backup_dir = ROOT / "WHATSAPP_CONVERSAS" / "Conversa Whatsapp"
    if backup_dir.exists():
        raw_dir = backup_dir
    # -----------------------------------

    proc_dir = ROOT / "data" / "processed"
    proc_dir.mkdir(parents=True, exist_ok=True)

    txt_files = list(raw_dir.glob("Conversa_do_WhatsApp_com_*.txt"))
    
    # --- COMPLEMENTO ADICIONADO AQUI ---
    # Fallback para buscar qualquer .txt caso o nome do arquivo de backup não tenha o prefixo exato
    if not txt_files:
        txt_files = list(raw_dir.glob("*.txt"))
    # -----------------------------------

    if not txt_files:
        print(f"\n⚠️  Nenhum arquivo de chat encontrado em: {raw_dir}")
        print(f"   Copie os arquivos .txt exportados do WhatsApp para esta pasta.")
        return

    print(f"\n🎷 SAMBA EXPORT — WhatsApp Parser")
    print(f"{'='*60}")
    print(f"📁 Diretório: {raw_dir}")
    print(f"📄 Arquivos encontrados: {len(txt_files)}\n")

    msgs = parse_all_chats(raw_dir)

    print(f"\n{'='*60}")
    print(f"📊 RESUMO")
    print(f"{'='*60}")
    print(f"  Total de mensagens:  {len(msgs):,}")
    print(f"  Com cotação:         {sum(1 for m in msgs if m.has_quote):,}")
    print(f"  Grupos:              {len(set(m.group_name for m in msgs))}")
    if msgs:
        print(f"  Período:             {msgs[0].timestamp.date()} a {msgs[-1].timestamp.date()}")

        # Stats por commodity
        commodities = {}
        for m in msgs:
            if m.commodity:
                commodities[m.commodity] = commodities.get(m.commodity, 0) + 1
        if commodities:
            print(f"\n  📦 Commodities detectadas:")
            for c, count in sorted(commodities.items(), key=lambda x: -x[1]):
                print(f"     {c}: {count} menções")

        # Stats por grupo
        groups = {}
        for m in msgs:
            if not m.is_system:
                groups[m.group_name] = groups.get(m.group_name, 0) + 1
        print(f"\n  💬 Mensagens por grupo:")
        for g, count in sorted(groups.items(), key=lambda x: -x[1])[:10]:
            quotes = sum(1 for m in msgs if m.group_name == g and m.has_quote)
            print(f"     {g}: {count} msgs ({quotes} cotações)")

    # Exportar
    print(f"\n{'='*60}")
    print(f"💾 EXPORTANDO...\n")
    export_quotes_csv(msgs, proc_dir / "cotacoes.csv")
    export_messages_json(msgs, proc_dir / "mensagens.json")

    # Summary JSON
    summary = {
        "parsed_at": datetime.now().isoformat(),
        "total_messages": len(msgs),
        "total_quotes": sum(1 for m in msgs if m.has_quote),
        "groups": len(set(m.group_name for m in msgs)),
        "commodities": commodities if msgs else {},
        "period_start": msgs[0].timestamp.isoformat() if msgs else None,
        "period_end": msgs[-1].timestamp.isoformat() if msgs else None,
    }
    with open(proc_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Parse concluído! Arquivos em: {proc_dir}")


def cmd_init_db():
    """Criar tabelas no banco de dados."""
    from models.database import create_tables
    print("\n🗄️  Criando banco de dados...")
    create_tables()
    print("✅ Banco criado com sucesso!")


def cmd_import():
    """Importar mensagens parseadas no banco de dados."""
    from models.database import create_tables, get_session, Message
    proc_dir = ROOT / "data" / "processed"
    json_file = proc_dir / "mensagens.json"

    if not json_file.exists():
        print("⚠️  Rode 'python main.py parse' primeiro.")
        return

    engine = create_tables()
    session = get_session(engine)

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    count = 0
    for item in data:
        msg = Message(
            timestamp=datetime.fromisoformat(item["timestamp"]),
            sender=item["sender"],
            content=item["content"],
            group_name=item["group_name"],
            is_media=item["is_media"],
            is_system=item["is_system"],
            commodity=item.get("commodity"),
            price=item.get("price"),
            currency=item.get("currency"),
            volume=item.get("volume"),
            volume_unit=item.get("volume_unit"),
            incoterm=item.get("incoterm"),
            has_quote=item.get("has_quote", False),
        )
        session.add(msg)
        count += 1

    session.commit()
    session.close()
    print(f"✅ {count:,} mensagens importadas no banco de dados!")


def cmd_create_deals():
    """
    Converte cotações do banco em Deals SEM chamar nenhuma API.
    Usa apenas os dados extraídos pelo regex do parser (price, volume, commodity).
    Nome: "[Commodity] Volume Unit - Sender". Risk_score=50 padrão.
    """
    from sqlalchemy import or_
    from models.database import create_tables, get_session, Message, Deal

    engine = create_tables()
    session = get_session(engine)

    # Mensagens com price OU volume que ainda não viraram Deal
    candidatas = session.query(Message).outerjoin(
        Deal, Message.id == Deal.source_message_id
    ).filter(
        Message.has_quote == True,
        Deal.id == None,
        or_(Message.price != None, Message.volume != None),
        Message.is_system == False,
        Message.is_media == False,
    ).order_by(Message.timestamp.asc()).all()

    if not candidatas:
        print("Nenhuma cotação nova para converter em Deal.")
        session.close()
        return

    print(f"\n📦 Convertendo {len(candidatas)} cotações em Deals (sem API)...\n")

    created = 0
    skipped = 0
    for msg in candidatas:
        commodity = msg.commodity or "Indefinida"
        volume    = msg.volume
        unit      = msg.volume_unit or "MT"

        # Nome amigável
        vol_str = f"{volume:.0f} {unit}" if volume else "Vol. indefinido"
        deal_name = f"[{commodity}] {vol_str} - {msg.sender}"

        deal = Deal(
            name=deal_name,
            commodity=commodity,
            volume=volume,
            volume_unit=unit,
            price=msg.price,
            currency=msg.currency or "USD",
            incoterm=msg.incoterm,
            origin=msg.location,
            stage="Lead Capturado",
            risk_score=50,
            source_group=msg.group_name,
            source_message_id=msg.id,
            source_sender=msg.sender,
            status="ativo",
        )
        session.add(deal)
        created += 1
        print(f"  + {deal_name}")

    session.commit()
    session.close()
    print(f"\n✅ {created} Deals criados | {skipped} ignorados")


def cmd_stats():
    """Mostrar estatísticas do banco."""
    from models.database import get_session, Message, Deal
    session = get_session()
    total = session.query(Message).count()
    quotes = session.query(Message).filter(Message.has_quote == True).count()
    deals = session.query(Deal).count()
    session.close()
    print(f"\n📊 Stats do banco:")
    print(f"  Mensagens: {total:,}")
    print(f"  Cotações:  {quotes:,}")
    print(f"  Deals:     {deals:,}")


def cmd_ingest_xlsx():
    """Ingerir deals do workbook Excel PEDIDOS SAMBA .xlsx."""
    from services.xlsx_ingestion import ingest_xlsx
    print("\nSAMBA EXPORT — Ingestion do Workbook Excel")
    print("=" * 60)
    result = ingest_xlsx()
    if result and isinstance(result, dict):
        print("\n" + "=" * 60)
        print("RESUMO FINAL:")
        total_deals = 0
        total_strategic = 0
        for sheet, stats in result.items():
            inserted   = stats.get("inserted", 0)
            strategic  = stats.get("inserted_strategic", 0)
            skipped    = stats.get("skipped", 0)
            errors     = stats.get("errors", 0)
            total_deals     += inserted
            total_strategic += strategic
            print(f"  {sheet}: {inserted} deals | {strategic} estrategicos | {skipped} skip | {errors} erros")
        print(f"\n  TOTAL: {total_deals} deals + {total_strategic} registros estrategicos")


def cmd_ingest_sheets():
    """Ingerir dados das planilhas HTML exportadas do Google Sheets."""
    from services.sheets_html_ingestion import ingest_all_sheets
    print("\nSAMBA EXPORT — Ingestion de Planilhas HTML")
    print("=" * 60)
    result = ingest_all_sheets()
    print("\n" + "=" * 60)
    print("✅ RESUMO FINAL:")
    total_deals = 0
    total_strategic = 0
    for fname, stats in result.items():
        inserted = stats.get("inserted", 0)
        strategic = stats.get("inserted_strategic", 0)
        skipped = stats.get("skipped", 0)
        errors = stats.get("errors", 0)
        total_deals += inserted
        total_strategic += strategic
        print(f"  {fname}: {inserted} deals | {strategic} estratégicos | {skipped} skip | {errors} erros")
    print(f"\n  TOTAL: {total_deals} deals + {total_strategic} registros estratégicos")


def cmd_dashboard():
    """Abrir dashboard Streamlit."""
    import subprocess
    dash = ROOT / "dashboards" / "streamlit_app.py"
    print("🚀 Abrindo Control Desk...")
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(dash)])


def main():
    commands = {
        "parse": cmd_parse,
        "init-db": cmd_init_db,
        "import": cmd_import,
        "create-deals": cmd_create_deals,
        "stats": cmd_stats,
        "ingest-xlsx": cmd_ingest_xlsx,
        "ingest-sheets": cmd_ingest_sheets,
        "dashboard": cmd_dashboard,
    }

    if len(sys.argv) < 2 or sys.argv[1] not in commands:
        print(__doc__)
        print(f"Comandos disponíveis: {', '.join(commands.keys())}")
        return

    commands[sys.argv[1]]()


if __name__ == "__main__":
    main()