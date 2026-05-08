"""
agents/base_agent.py
====================
Classe base abstrata para todos os agentes do Samba Export Control Desk.

Todos os agentes concretos devem herdar de BaseAgent e implementar process().

Padrão de uso:
    class MeuAgente(BaseAgent):
        name = "MeuAgente"
        description = "Faz X"

        def process(self, data=None) -> dict:
            ...
            return {"status": "success", ...}

    agente = MeuAgente()
    resultado = agente.run()          # run() chama process() com tratamento de erro
    print(agente.get_status())
"""

from __future__ import annotations

import json
import logging
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Garantir path do projeto no sys.path quando rodado como script
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LOGS_DIR = Path("data/logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class BaseAgent(ABC):
    """
    Classe base para todos os agentes de IA do Samba Export.

    Attributes:
        name:                  Identificador único do agente (snake_case).
        description:           Descrição do papel e responsabilidade.
        visible_in_groups:     Se True, este agente está ativo nos grupos WhatsApp.
        generates_spreadsheets: Se True, este agente produz planilhas/CSVs.
    """

    name: str = "BaseAgent"
    description: str = "Agente base — não instanciar diretamente"
    visible_in_groups: bool = False
    generates_spreadsheets: bool = False

    def __init__(self):
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        self._run_count: int = 0
        self._last_run: Optional[datetime] = None
        self._last_result: Optional[dict] = None
        self._logger = logging.getLogger(f"samba.agents.{self.name}")

    # ──────────────────────────────────────────────────────────
    # Interface pública obrigatória
    # ──────────────────────────────────────────────────────────

    @abstractmethod
    def process(self, data: Any = None) -> dict:
        """
        Executa a tarefa principal do agente.

        Deve retornar um dict com pelo menos:
            status: "success" | "error" | "skipped"

        Qualquer exceção não capturada aqui será tratada por run().
        """

    # ──────────────────────────────────────────────────────────
    # Runner com tratamento de erros
    # ──────────────────────────────────────────────────────────

    def run(self, data: Any = None) -> dict:
        """
        Ponto de entrada público — chama process() com contadores e error handling.

        Use este método em vez de chamar process() diretamente.
        """
        self._run_count += 1
        self._last_run = datetime.now()
        self.log_action("run_started", {"run_count": self._run_count})

        try:
            result = self.process(data)
            if not isinstance(result, dict):
                result = {"status": "success", "result": result}
            result.setdefault("agent", self.name)
            result.setdefault("timestamp", datetime.now().isoformat())
            result.setdefault("status", "success")
        except Exception as exc:
            self._logger.exception("Erro não tratado em %s.process()", self.name)
            result = {
                "status": "error",
                "agent": self.name,
                "timestamp": datetime.now().isoformat(),
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
            self.log_action("run_error", {"error": str(exc), "type": type(exc).__name__}, level="ERROR")

        self._last_result = result
        self.log_action("run_finished", {
            "status": result.get("status"),
            "run_count": self._run_count,
        })
        return result

    # ──────────────────────────────────────────────────────────
    # Logging persistente
    # ──────────────────────────────────────────────────────────

    def log_action(
        self,
        action: str,
        details: dict | None = None,
        level: str = "INFO",
    ) -> None:
        """
        Registra uma ação em data/logs/<AgentName>_YYYY-MM-DD.jsonl

        Cada linha do arquivo é um JSON independente (JSON Lines format).

        Args:
            action:  Descrição curta da ação (ex: "deal_assigned", "message_sent").
            details: Dados complementares em dict (opcional).
            level:   "DEBUG" | "INFO" | "WARNING" | "ERROR"
        """
        now = datetime.now()
        entry = {
            "ts": now.isoformat(timespec="seconds"),
            "agent": self.name,
            "action": action,
            "level": level,
            "details": details or {},
        }

        # Logger Python padrão
        log_fn = getattr(self._logger, level.lower(), self._logger.info)
        log_fn("[%s] %s", action, json.dumps(details or {}, ensure_ascii=False))

        # Arquivo JSONL diário
        log_file = LOGS_DIR / f"{self.name}_{now.strftime('%Y-%m-%d')}.jsonl"
        try:
            with open(log_file, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as exc:
            self._logger.error("Falha ao persistir log: %s", exc)

    # ──────────────────────────────────────────────────────────
    # Introspection
    # ──────────────────────────────────────────────────────────

    def get_status(self) -> dict:
        """
        Retorna o estado atual do agente para exibição no dashboard.

        Returns:
            dict com: name, description, visible_in_groups,
                      generates_spreadsheets, run_count,
                      last_run (ISO), last_result_status
        """
        return {
            "name": self.name,
            "description": self.description,
            "visible_in_groups": self.visible_in_groups,
            "generates_spreadsheets": self.generates_spreadsheets,
            "run_count": self._run_count,
            "last_run": self._last_run.isoformat() if self._last_run else None,
            "last_result_status": (
                self._last_result.get("status") if self._last_result else None
            ),
        }

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"name={self.name!r} "
            f"runs={self._run_count} "
            f"last={self._last_run.strftime('%H:%M:%S') if self._last_run else 'never'}>"
        )
