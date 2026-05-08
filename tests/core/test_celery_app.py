"""
Testes da configuração do Celery — sem broker real.

Validam que o app foi registrado com filas, beat schedule, serialização JSON
e roteamento por nome canônico de task. NÃO conectamos ao Redis aqui.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from core.celery_app import (
    BEAT_SYNC_MINUTES,
    QUEUE_EXTRACTOR,
    QUEUE_INBOUND,
    QUEUE_SYNC,
    TASK_EXTRACT_MESSAGE,
    TASK_PROCESS_INBOUND,
    TASK_SYNC_SPREADSHEET,
    celery_app,
)


def test_celery_app_basics():
    assert celery_app.main == "samba_core"
    conf = celery_app.conf
    assert conf.task_serializer == "json"
    assert "json" in conf.accept_content
    assert conf.task_acks_late is True
    assert conf.task_reject_on_worker_lost is True
    assert conf.worker_prefetch_multiplier == 1
    assert conf.timezone == "America/Sao_Paulo"


def test_task_routes_per_queue():
    routes = celery_app.conf.task_routes
    assert routes[TASK_PROCESS_INBOUND]["queue"] == QUEUE_INBOUND
    assert routes[TASK_EXTRACT_MESSAGE]["queue"] == QUEUE_EXTRACTOR
    assert routes[TASK_SYNC_SPREADSHEET]["queue"] == QUEUE_SYNC


def test_beat_schedule_sync_every_n_minutes():
    schedule = celery_app.conf.beat_schedule
    entry = schedule["sync-spreadsheet-to-drive"]
    assert entry["task"] == TASK_SYNC_SPREADSHEET
    assert entry["options"]["queue"] == QUEUE_SYNC
    # schedule é um crontab — validamos o minuto declarado.
    assert str(BEAT_SYNC_MINUTES) in repr(entry["schedule"])
