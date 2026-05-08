"""Master Data Management — atlas de produtos da Samba Export."""
from taxonomy.resolver import (
    DEFAULT_MAPPING_PATH,
    ProductResolver,
    ResolvedProduct,
    normalize_text,
)

__all__ = [
    "DEFAULT_MAPPING_PATH",
    "ProductResolver",
    "ResolvedProduct",
    "normalize_text",
]
