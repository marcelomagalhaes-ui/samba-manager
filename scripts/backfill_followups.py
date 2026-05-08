"""
scripts/backfill_followups.py
==============================
Cria registros FollowUp para todos os deals em Qualificação que ainda
não têm nenhum follow-up pendente ou enviado.

Usado para popular o painel após a feature de follow-up ser ativada
em uma base de dados já existente.

Uso:
    python scripts/backfill_followups.py             # cria os registros
    python scripts/backfill_followups.py --dry-run   # só lista, não insere
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from models.database import Deal, FollowUp, get_session


def backfill(dry_run: bool = False) -> int:
    session = get_session()
    try:
        # Todos os deals ativos em Qualificação
        deals_qual = (
            session.query(Deal)
            .filter(Deal.status == "ativo", Deal.stage == "Qualificação")
            .all()
        )

        # IDs que já têm follow-up ativo (pendente ou enviado)
        existing = {
            fu.deal_id
            for fu in session.query(FollowUp)
            .filter(FollowUp.status.in_(["pendente", "enviado"]))
            .all()
        }

        criados = 0
        for deal in deals_qual:
            if deal.id in existing:
                continue  # já tem follow-up ativo, pula

            # Mensagem padrão de rastreamento
            msg = (
                f"Olá! Seguindo nossa conversa sobre {deal.commodity or 'a commodity'}, "
                f"precisamos de algumas informações para avançar com o negócio. "
                f"Pode nos ajudar com os dados pendentes?"
            )

            if not dry_run:
                fu = FollowUp(
                    deal_id=deal.id,
                    # target_person = parceiro externo (número E.164 quando disponível)
                    target_person=deal.source_sender or None,
                    # target_group  = nome do grupo WhatsApp de origem (referência)
                    target_group=deal.source_group or None,
                    message=msg,
                    due_at=datetime.utcnow() + timedelta(minutes=5),
                    status="pendente",
                )
                session.add(fu)

            criados += 1
            print(
                f"  {'[DRY] ' if dry_run else ''}FollowUp para Deal #{deal.id} "
                f"'{deal.name}' | {deal.commodity} | {deal.assignee}"
            )

        if not dry_run:
            session.commit()

        return criados
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill FollowUps para deals em Qualificacao")
    parser.add_argument("--dry-run", action="store_true", help="Lista sem inserir")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"BACKFILL FOLLOWUPS {'(DRY RUN)' if args.dry_run else ''}")
    print(f"{'='*60}\n")

    n = backfill(dry_run=args.dry_run)

    print(f"\n{'='*60}")
    if args.dry_run:
        print(f"Total que seria criado: {n} follow-ups")
    else:
        print(f"Follow-ups criados: {n}")
    print(f"{'='*60}\n")
