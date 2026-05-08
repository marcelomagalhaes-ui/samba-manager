"""Pipeline de conciliação Sheets → Drive da Samba Export."""
from sync.exceptions import (
    CircuitBreakerOpen,
    LLMDegradedResponse,
    LLMTransportError,
    LLMUnavailable,
)
from sync.llm_gateway import CircuitBreaker, LLMGateway
from sync.models import (
    SHEET_COLUMN_COUNT,
    DealRow,
    IngestionResult,
    RowRejection,
    ingest_sheet_rows,
)
from sync.status import SyncStatus
from sync.status_writer import SheetStatusWriter

__all__ = [
    # exceptions
    "CircuitBreakerOpen",
    "LLMDegradedResponse",
    "LLMTransportError",
    "LLMUnavailable",
    # llm gateway
    "CircuitBreaker",
    "LLMGateway",
    # models
    "SHEET_COLUMN_COUNT",
    "DealRow",
    "IngestionResult",
    "RowRejection",
    "ingest_sheet_rows",
    # status
    "SyncStatus",
    "SheetStatusWriter",
]
