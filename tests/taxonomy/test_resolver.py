"""
Testes do ProductResolver.
Garantem que o comportamento do monolito é preservado byte-a-byte
após a extração do atlas para YAML.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from taxonomy import ProductResolver, ResolvedProduct, normalize_text


CORE_ROOT = "ROOT_CORE_FAKE_ID"
OTHER_ROOT = "ROOT_NEGOCIOS_FAKE_ID"


@pytest.fixture(scope="module")
def resolver() -> ProductResolver:
    return ProductResolver.from_default(
        core_root_id=CORE_ROOT,
        other_root_id=OTHER_ROOT,
    )


# ---------------------------------------------------------------------------
# Normalização (idêntica ao monolito)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("AÇÚCAR", "acucar"),
        ("  Açúcar  ", "acucar"),
        ("Pork Belly", "pork belly"),
        ("CAFÉ", "cafe"),
        (None, ""),
        ("", ""),
    ],
)
def test_normalize_text(raw, expected):
    assert normalize_text(raw) == expected


# ---------------------------------------------------------------------------
# Commodities Core — vão para CORE_ROOT
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, canonical_path, leaf",
    [
        ("frango", "FRANGO", "FRANGO"),
        ("Chicken Paw", "FRANGO", "FRANGO"),
        ("pork belly", "SUÍNOS/BELLY", "BELLY"),
        ("Barriga", "SUÍNOS/BELLY", "BELLY"),
        ("acem", "BOVINOS/ACÉM", "ACÉM"),
        ("Milho Branco", "MILHO/MILHO BRANCO", "MILHO BRANCO"),
        ("soja non gmo", "SOJA/NON GMO", "NON GMO"),
        ("AÇÚCAR", "AÇÚCAR", "AÇÚCAR"),
        ("VHP", "AÇÚCAR/VHP", "VHP"),
        ("Café", "CAFÉ", "CAFÉ"),
        ("coffee", "CAFÉ", "CAFÉ"),
    ],
)
def test_resolves_core_commodities(resolver, raw, canonical_path, leaf):
    r = resolver.resolve(raw)
    assert r.matched is True
    assert r.is_core is True
    assert r.canonical_path == canonical_path
    assert r.leaf_name == leaf
    assert r.root_folder_id == CORE_ROOT


# ---------------------------------------------------------------------------
# Não-Core (genéricos e financeiros) — vão para OTHER_ROOT
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, canonical_path, leaf",
    [
        ("frutas", "FRUTAS", "FRUTAS"),
        ("fruits", "FRUTAS", "FRUTAS"),
        ("coco", "COCO", "COCO"),
        ("CPR", "CPR", "CPR"),
        ("credito rural", "CRÉDITO RURAL", "CRÉDITO RURAL"),
        ("sblc", "SBLC/DLC", "DLC"),
        ("LC", "SBLC/DLC", "DLC"),
    ],
)
def test_resolves_non_core(resolver, raw, canonical_path, leaf):
    r = resolver.resolve(raw)
    assert r.matched is True
    assert r.is_core is False
    assert r.canonical_path == canonical_path
    assert r.leaf_name == leaf
    assert r.root_folder_id == OTHER_ROOT


# ---------------------------------------------------------------------------
# Fallback — produto desconhecido preserva comportamento do monolito
# ---------------------------------------------------------------------------

def test_unknown_product_falls_back_to_negocios(resolver):
    r = resolver.resolve("Widget Exótico X-17")
    assert r.matched is False
    assert r.is_core is False
    assert r.root_folder_id == OTHER_ROOT
    # Monolito fazia: nome_planilha.strip().upper()
    assert r.canonical_path == "WIDGET EXÓTICO X-17"
    assert r.leaf_name == "WIDGET EXÓTICO X-17"


def test_empty_raw_name_produces_placeholder(resolver):
    r = resolver.resolve("")
    assert r.matched is False
    assert r.leaf_name == "PARCEIRO N/D"


# ---------------------------------------------------------------------------
# folder_segments — suportam a construção recursiva da árvore no Drive
# ---------------------------------------------------------------------------

def test_folder_segments_two_levels(resolver):
    r = resolver.resolve("picanha")
    assert r.folder_segments == ("BOVINOS", "PICANHA")
    assert r.top_category == "BOVINOS"


def test_folder_segments_single_level(resolver):
    r = resolver.resolve("arroz")
    assert r.folder_segments == ("ARROZ",)
    assert r.top_category == "ARROZ"


# ---------------------------------------------------------------------------
# Índice carrega todos os aliases sem colisão silenciosa
# ---------------------------------------------------------------------------

def test_canonical_name_resolves_to_itself(resolver):
    # Nome canônico digitado como está no YAML também é resolvível.
    r = resolver.resolve("FRANGO")
    assert r.matched is True
    assert r.canonical_path == "FRANGO"


def test_returns_frozen_dataclass(resolver):
    r = resolver.resolve("soja")
    assert isinstance(r, ResolvedProduct)
    with pytest.raises(Exception):
        r.canonical_path = "outro"  # type: ignore[misc]
