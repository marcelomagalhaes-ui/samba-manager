"""
taxonomy/resolver.py
====================
Resolve um produto "sujo" vindo da planilha para sua identidade canônica
no atlas de mercado da Samba Export.

É pura: não toca Drive, não toca Sheets, não faz IO além de ler o YAML
uma única vez no construtor. Totalmente testável em milissegundos.

Uso típico:
    resolver = ProductResolver.from_default(
        core_root_id=SAMBA_ROOT_FOLDER_ID,
        other_root_id=SAMBA_NEGOCIOS_FOLDER_ID,
    )
    result = resolver.resolve("Pork Belly")
    # result.canonical_path      -> "SUÍNOS/BELLY"
    # result.folder_segments     -> ("SUÍNOS", "BELLY")
    # result.leaf_name           -> "BELLY"
    # result.root_folder_id      -> SAMBA_ROOT_FOLDER_ID
    # result.is_core             -> True
    # result.matched             -> True
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import yaml


DEFAULT_MAPPING_PATH = Path(__file__).parent / "mapping.yaml"


@dataclass(frozen=True)
class ResolvedProduct:
    """Identidade canônica de um produto após consulta ao atlas."""
    canonical_path: str
    folder_segments: tuple[str, ...]
    leaf_name: str
    root_folder_id: str
    is_core: bool
    matched: bool

    @property
    def top_category(self) -> str:
        return self.folder_segments[0]


def normalize_text(text: str | None) -> str:
    """Lower + strip + ASCII (sem acentos). Consistente com o monolito."""
    if not text:
        return ""
    t = str(text).strip().lower()
    return unicodedata.normalize("NFKD", t).encode("ASCII", "ignore").decode("utf-8")


class ProductResolver:
    """Carrega o atlas uma vez e devolve `ResolvedProduct` para strings brutas."""

    def __init__(
        self,
        products: dict,
        core_root_id: str,
        other_root_id: str,
    ) -> None:
        self._core_root_id = core_root_id
        self._other_root_id = other_root_id
        self._alias_index: dict[str, tuple[str, ...]] = {}
        self._is_core: dict[str, bool] = {}
        self._build_index(products)

    @classmethod
    def from_yaml(
        cls,
        path: str | Path,
        core_root_id: str,
        other_root_id: str,
    ) -> "ProductResolver":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        products = data.get("products", {}) or {}
        return cls(products, core_root_id, other_root_id)

    @classmethod
    def from_default(
        cls,
        core_root_id: str,
        other_root_id: str,
    ) -> "ProductResolver":
        return cls.from_yaml(DEFAULT_MAPPING_PATH, core_root_id, other_root_id)

    # -----------------------------------------------------------------
    # Construção do índice
    # -----------------------------------------------------------------

    def _build_index(self, products: dict) -> None:
        for canonical_parent, cfg in products.items():
            is_core = bool((cfg or {}).get("core", False))
            self._is_core[canonical_parent] = is_core

            for alias in (cfg or {}).get("aliases", []) or []:
                self._register_alias(alias, (canonical_parent,))

            subs = (cfg or {}).get("subcategories", {}) or {}
            for canonical_child, sub_cfg in subs.items():
                for alias in (sub_cfg or {}).get("aliases", []) or []:
                    self._register_alias(alias, (canonical_parent, canonical_child))

            # O próprio nome canônico (ex: "FRANGO") também é um alias válido.
            self._register_alias(canonical_parent, (canonical_parent,))

    def _register_alias(self, alias: str, segments: tuple[str, ...]) -> None:
        key = normalize_text(alias)
        if not key:
            return
        # Primeira ocorrência vence — protege contra colisões silenciosas no YAML.
        self._alias_index.setdefault(key, segments)

    # -----------------------------------------------------------------
    # API pública
    # -----------------------------------------------------------------

    def resolve(self, raw_name: str) -> ResolvedProduct:
        key = normalize_text(raw_name)
        segments = self._alias_index.get(key)

        if segments is None:
            # Fallback: replica o monolito — devolve UPPER e roteia para Negócios.
            fallback = (raw_name or "").strip().upper() or "PARCEIRO N/D"
            return ResolvedProduct(
                canonical_path=fallback,
                folder_segments=(fallback,),
                leaf_name=fallback,
                root_folder_id=self._other_root_id,
                is_core=False,
                matched=False,
            )

        top = segments[0]
        is_core = self._is_core.get(top, False)
        return ResolvedProduct(
            canonical_path="/".join(segments),
            folder_segments=segments,
            leaf_name=segments[-1],
            root_folder_id=self._core_root_id if is_core else self._other_root_id,
            is_core=is_core,
            matched=True,
        )

    # Utilitário: lista todos os aliases conhecidos (útil em testes/debug).
    def known_aliases(self) -> Iterable[str]:
        return self._alias_index.keys()
