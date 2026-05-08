"""
sync/status.py
==============
Status que é espelhado na coluna O ("STATUS_AUTOMACAO") da aba "todos andamento".

O operador abre a planilha e vê o estado imediatamente — sem consultar logs.
"""
from enum import Enum


class SyncStatus(str, Enum):
    OK = "OK"
    """PDF gerado e arquivado com sucesso no Deal Room."""

    PENDING_IA = "PENDING_IA"
    """Linha aguarda reprocessamento — IA indisponível no momento da tentativa."""

    REJECTED = "REJECTED"
    """Linha malformada ou incompleta — requer intervenção humana."""

    SKIPPED = "SKIPPED"
    """Linha em branco ou sem produto — sem ação."""

    def __str__(self) -> str:  # para escrita limpa na célula
        return self.value
